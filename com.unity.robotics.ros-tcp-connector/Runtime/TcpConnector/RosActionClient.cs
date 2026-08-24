using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector
{
    public enum RosActionClientState
    {
        WaitingForConnection,
        Ready,
        Unsupported,
        Faulted,
    }

    public enum RosActionGoalStatus
    {
        Unknown = 0,
        Accepted = 1,
        Executing = 2,
        Canceling = 3,
        Succeeded = 4,
        Canceled = 5,
        Aborted = 6,
    }

    public enum RosActionCancelReturnCode
    {
        None = 0,
        Rejected = 1,
        UnknownGoalId = 2,
        GoalTerminated = 3,
    }

    public sealed class RosActionException : Exception
    {
        public string Code { get; }

        public RosActionException(string code, string message) : base(message)
        {
            Code = code;
        }
    }

    public sealed class RosActionResult<TResult> where TResult : Message
    {
        public RosActionGoalStatus Status { get; }
        public TResult Result { get; }

        public RosActionResult(RosActionGoalStatus status, TResult result)
        {
            Status = status;
            Result = result;
        }
    }

    public sealed class RosActionCancelResponse
    {
        public RosActionCancelReturnCode ReturnCode { get; }
        public bool Accepted => ReturnCode == RosActionCancelReturnCode.None;

        public RosActionCancelResponse(RosActionCancelReturnCode returnCode)
        {
            ReturnCode = returnCode;
        }
    }

    public interface IRosActionGoal<TResult> where TResult : Message
    {
        string GoalId { get; }
        string RosGoalId { get; }
        RosActionGoalStatus Status { get; }
        Task<RosActionResult<TResult>> Result { get; }
        Task<RosActionCancelResponse> CancelAsync();
    }

    public sealed class RosActionGoalResponse<TResult> where TResult : Message
    {
        public bool Accepted { get; }
        public IRosActionGoal<TResult> Goal { get; }

        public RosActionGoalResponse(bool accepted, IRosActionGoal<TResult> goal)
        {
            Accepted = accepted;
            Goal = goal;
        }
    }

    public interface IRosActionClient<TGoal, TResult, TFeedback>
        where TGoal : Message
        where TResult : Message
        where TFeedback : Message
    {
        RosActionClientState State { get; }
        string LastError { get; }
        event Action<RosActionClientState> StateChanged;
        Task<RosActionGoalResponse<TResult>> SendGoalAsync(
            TGoal goal, Action<TFeedback> feedbackCallback = null);
    }

    internal interface IRosActionClientInternal
    {
        string ActionName { get; }
        string RosMessageName { get; }
        void ResolveEndpointCapability(bool supported);
        void OnRegistered();
        void OnGoalResponse(SysCommand_ActionGoalResponse response);
        void OnFeedback(string goalId, byte[] data);
        void OnResult(SysCommand_ActionResult result, byte[] data);
        void OnCancelResponse(SysCommand_ActionCancelResponse response);
        void OnError(SysCommand_ActionError error);
        void OnConnectionLost();
    }

    internal sealed class RosActionGoal<TResult> : IRosActionGoal<TResult>
        where TResult : Message
    {
        readonly Func<string, Task<RosActionCancelResponse>> m_Cancel;
        readonly TaskCompletionSource<RosActionResult<TResult>> m_Result =
            new TaskCompletionSource<RosActionResult<TResult>>();

        public string GoalId { get; }
        public string RosGoalId { get; internal set; }
        public RosActionGoalStatus Status { get; internal set; }
        public Task<RosActionResult<TResult>> Result => m_Result.Task;

        internal RosActionGoal(string goalId, Func<string, Task<RosActionCancelResponse>> cancel)
        {
            GoalId = goalId;
            m_Cancel = cancel;
            Status = RosActionGoalStatus.Unknown;
        }

        public Task<RosActionCancelResponse> CancelAsync()
        {
            return m_Cancel(GoalId);
        }

        internal void Complete(RosActionGoalStatus status, TResult result)
        {
            Status = status;
            m_Result.TrySetResult(new RosActionResult<TResult>(status, result));
        }

        internal void Fail(Exception exception)
        {
            m_Result.TrySetException(exception);
        }
    }

    internal sealed class RosActionClient<TGoal, TResult, TFeedback> :
        IRosActionClient<TGoal, TResult, TFeedback>, IRosActionClientInternal
        where TGoal : Message
        where TResult : Message
        where TFeedback : Message
    {
        sealed class PendingGoal
        {
            public Action<TFeedback> Feedback;
            public RosActionGoal<TResult> Goal;
            public TaskCompletionSource<RosActionGoalResponse<TResult>> Response =
                new TaskCompletionSource<RosActionGoalResponse<TResult>>();
            public TaskCompletionSource<RosActionCancelResponse> Cancel;

            public void Fail(Exception exception)
            {
                Response.TrySetException(exception);
                Cancel?.TrySetException(exception);
                Goal.Fail(exception);
            }
        }

        readonly ROSConnection m_Connection;
        readonly Dictionary<string, PendingGoal> m_Goals = new Dictionary<string, PendingGoal>();

        public string ActionName { get; }
        public string RosMessageName { get; }
        public RosActionClientState State { get; private set; } = RosActionClientState.WaitingForConnection;
        public string LastError { get; private set; }
        public event Action<RosActionClientState> StateChanged;

        internal RosActionClient(ROSConnection connection, string actionName, string rosMessageName)
        {
            m_Connection = connection;
            ActionName = actionName;
            RosMessageName = rosMessageName;
        }

        public Task<RosActionGoalResponse<TResult>> SendGoalAsync(
            TGoal goal, Action<TFeedback> feedbackCallback = null)
        {
            m_Connection.AssertActionMainThread();
            if (goal == null)
                throw new ArgumentNullException(nameof(goal));
            if (State != RosActionClientState.Ready)
                throw new RosActionException("action_not_ready", ActionName + " is not ready: " + State);

            string goalId = Guid.NewGuid().ToString("N");
            var pending = new PendingGoal { Feedback = feedbackCallback };
            pending.Goal = new RosActionGoal<TResult>(goalId, CancelGoalAsync);
            m_Goals.Add(goalId, pending);
            m_Connection.QueueActionGoal(ActionName, goalId, goal);
            return pending.Response.Task;
        }

        Task<RosActionCancelResponse> CancelGoalAsync(string goalId)
        {
            m_Connection.AssertActionMainThread();
            PendingGoal pending;
            if (!m_Goals.TryGetValue(goalId, out pending))
                throw new RosActionException("unknown_goal", "Goal is no longer active");
            if (pending.Goal.Status == RosActionGoalStatus.Unknown)
                throw new RosActionException("goal_pending", "Goal acceptance is still pending");
            if (pending.Cancel != null)
                return pending.Cancel.Task;

            pending.Cancel = new TaskCompletionSource<RosActionCancelResponse>();
            m_Connection.QueueActionCancel(ActionName, goalId);
            return pending.Cancel.Task;
        }

        public void ResolveEndpointCapability(bool supported)
        {
            if (!supported)
            {
                SetState(RosActionClientState.Unsupported,
                    "ROS-TCP-Endpoint does not advertise ros2_actions_v1");
                FailAll(new RosActionException("unsupported_endpoint", LastError));
                return;
            }

            SetState(RosActionClientState.WaitingForConnection, null);
            m_Connection.QueueActionRegistration(ActionName, RosMessageName);
        }

        public void OnRegistered()
        {
            SetState(RosActionClientState.Ready, null);
        }

        public void OnGoalResponse(SysCommand_ActionGoalResponse response)
        {
            PendingGoal pending;
            if (!m_Goals.TryGetValue(response.goal_id, out pending))
                return;

            if (!response.accepted)
            {
                pending.Response.TrySetResult(new RosActionGoalResponse<TResult>(false, null));
                m_Goals.Remove(response.goal_id);
                return;
            }

            pending.Goal.RosGoalId = response.ros_goal_id;
            pending.Goal.Status = RosActionGoalStatus.Accepted;
            pending.Response.TrySetResult(new RosActionGoalResponse<TResult>(true, pending.Goal));
        }

        public void OnFeedback(string goalId, byte[] data)
        {
            PendingGoal pending;
            if (!m_Goals.TryGetValue(goalId, out pending))
                return;
            pending.Goal.Status = RosActionGoalStatus.Executing;
            TFeedback feedback;
            try
            {
                feedback = m_Connection.DeserializeActionMessage<TFeedback>(
                    data, MessageSubtopic.Feedback);
            }
            catch (Exception exception)
            {
                pending.Fail(new RosActionException("malformed_feedback", exception.Message));
                m_Goals.Remove(goalId);
                return;
            }
            try
            {
                pending.Feedback?.Invoke(feedback);
            }
            catch (Exception exception)
            {
                Debug.LogException(exception);
            }
        }

        public void OnResult(SysCommand_ActionResult result, byte[] data)
        {
            PendingGoal pending;
            if (!m_Goals.TryGetValue(result.goal_id, out pending))
                return;
            TResult message;
            try
            {
                message = m_Connection.DeserializeActionMessage<TResult>(
                    data, MessageSubtopic.Result);
            }
            catch (Exception exception)
            {
                pending.Fail(new RosActionException("malformed_result", exception.Message));
                m_Goals.Remove(result.goal_id);
                return;
            }
            var status = (RosActionGoalStatus)result.status;
            pending.Goal.Complete(status, message);
            if (pending.Cancel != null && !pending.Cancel.Task.IsCompleted)
            {
                var returnCode = status == RosActionGoalStatus.Canceled
                    ? RosActionCancelReturnCode.None
                    : RosActionCancelReturnCode.GoalTerminated;
                pending.Cancel.TrySetResult(new RosActionCancelResponse(returnCode));
            }
            m_Goals.Remove(result.goal_id);
        }

        public void OnCancelResponse(SysCommand_ActionCancelResponse response)
        {
            PendingGoal pending;
            if (!m_Goals.TryGetValue(response.goal_id, out pending) || pending.Cancel == null)
                return;
            var cancelResponse = new RosActionCancelResponse(
                (RosActionCancelReturnCode)response.return_code);
            if (cancelResponse.Accepted)
                pending.Goal.Status = RosActionGoalStatus.Canceling;
            pending.Cancel.TrySetResult(cancelResponse);
        }

        public void OnError(SysCommand_ActionError error)
        {
            var exception = new RosActionException(error.code, error.message);
            if (string.IsNullOrEmpty(error.goal_id))
            {
                SetState(RosActionClientState.Faulted, error.message);
                FailAll(exception);
                return;
            }

            PendingGoal pending;
            if (!m_Goals.TryGetValue(error.goal_id, out pending))
                return;
            pending.Fail(exception);
            m_Goals.Remove(error.goal_id);
        }

        public void OnConnectionLost()
        {
            SetState(RosActionClientState.WaitingForConnection, "TCP connection lost");
            FailAll(new RosActionException("connection_lost",
                "TCP connection was lost; the remote ROS goal was not canceled"));
        }

        void FailAll(Exception exception)
        {
            foreach (PendingGoal pending in m_Goals.Values)
                pending.Fail(exception);
            m_Goals.Clear();
        }

        void SetState(RosActionClientState state, string error)
        {
            LastError = error;
            if (State == state)
                return;
            State = state;
            if (StateChanged == null)
                return;
            foreach (Action<RosActionClientState> callback in StateChanged.GetInvocationList())
            {
                try
                {
                    callback(state);
                }
                catch (Exception exception)
                {
                    Debug.LogException(exception);
                }
            }
        }
    }
}
