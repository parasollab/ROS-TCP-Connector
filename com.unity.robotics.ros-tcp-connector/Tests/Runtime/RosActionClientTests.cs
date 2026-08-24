using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Unity.Robotics.ROSTCPConnector;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;

namespace UnitTests
{
    public class RosActionClientTests
    {
        const string k_RosActionName = "test_msgs/TestAction";

        class GoalMsg : Message
        {
            public int value;
            public override void SerializeTo(MessageSerializer serializer) => serializer.Write(value);
            public static GoalMsg Deserialize(MessageDeserializer deserializer)
            {
                deserializer.Read(out int value);
                return new GoalMsg { value = value };
            }
        }

        class ResultMsg : Message
        {
            public int value;
            public override void SerializeTo(MessageSerializer serializer) => serializer.Write(value);
            public static ResultMsg Deserialize(MessageDeserializer deserializer)
            {
                deserializer.Read(out int value);
                return new ResultMsg { value = value };
            }
        }

        class FeedbackMsg : Message
        {
            public int value;
            public override void SerializeTo(MessageSerializer serializer) => serializer.Write(value);
            public static FeedbackMsg Deserialize(MessageDeserializer deserializer)
            {
                deserializer.Read(out int value);
                return new FeedbackMsg { value = value };
            }
        }

        ROSConnection m_Connection;
        ConcurrentQueue<Tuple<string, byte[]>> m_Incoming;

        [SetUp]
        public void SetUp()
        {
            MessageRegistry.Register(k_RosActionName, GoalMsg.Deserialize, MessageSubtopic.Goal);
            MessageRegistry.Register(k_RosActionName, ResultMsg.Deserialize, MessageSubtopic.Result);
            MessageRegistry.Register(k_RosActionName, FeedbackMsg.Deserialize, MessageSubtopic.Feedback);
            m_Connection = ROSConnection.GetOrCreateInstance();
            m_Connection.ConnectOnStart = false;
            m_Incoming = (ConcurrentQueue<Tuple<string, byte[]>>)typeof(ROSConnection)
                .GetField("m_IncomingMessages", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(m_Connection);
            while (m_Incoming.TryDequeue(out _)) { }
        }

        [Test]
        public void ActionLifecycle_DispatchesUserVisibleEventsOnUpdateThread()
        {
            int mainThread = Thread.CurrentThread.ManagedThreadId;
            string actionName = "/test_action_" + Guid.NewGuid().ToString("N");
            var client = m_Connection.RegisterActionClient<GoalMsg, ResultMsg, FeedbackMsg>(
                actionName);
            var dispatch = (IRosActionClientInternal)client;
            dispatch.ResolveEndpointCapability(true);
            Enqueue(SysCommand.k_SysCommand_ActionRegistered,
                new SysCommand_ActionRegistration { action_name = actionName });
            UpdateConnection();
            Assert.AreEqual(RosActionClientState.Ready, client.State);

            int outgoingBeforeGoal = OutgoingQueueCount();
            int feedbackThread = -1;
            var acceptance = client.SendGoalAsync(
                new GoalMsg { value = 7 },
                feedback => feedbackThread = Thread.CurrentThread.ManagedThreadId);
            Assert.AreEqual(outgoingBeforeGoal + 1, OutgoingQueueCount(),
                "A goal command and its payload must occupy one outgoing queue item");

            string goalId = GetOnlyGoalId(client);
            int acceptanceThread = -1;
            acceptance.ContinueWith(
                _ => acceptanceThread = Thread.CurrentThread.ManagedThreadId,
                TaskContinuationOptions.ExecuteSynchronously);
            Enqueue(SysCommand.k_SysCommand_ActionGoalResponse,
                new SysCommand_ActionGoalResponse
                {
                    action_name = actionName,
                    goal_id = goalId,
                    accepted = true,
                    ros_goal_id = "001122",
                });
            UpdateConnection();
            Assert.AreEqual(mainThread, acceptanceThread);
            Assert.IsTrue(acceptance.IsCompletedSuccessfully);

            IRosActionGoal<ResultMsg> goal = acceptance.Result.Goal;
            int resultThread = -1;
            goal.Result.ContinueWith(
                _ => resultThread = Thread.CurrentThread.ManagedThreadId,
                TaskContinuationOptions.ExecuteSynchronously);

            Enqueue(SysCommand.k_SysCommand_ActionFeedback,
                new SysCommand_ActionGoal { action_name = actionName, goal_id = goalId });
            EnqueuePayload(actionName, new FeedbackMsg { value = 42 });
            UpdateConnection();
            Assert.AreEqual(mainThread, feedbackThread);
            Assert.AreEqual(RosActionGoalStatus.Executing, goal.Status);

            Task<RosActionCancelResponse> cancellation = goal.CancelAsync();
            int cancellationThread = -1;
            cancellation.ContinueWith(
                _ => cancellationThread = Thread.CurrentThread.ManagedThreadId,
                TaskContinuationOptions.ExecuteSynchronously);
            Enqueue(SysCommand.k_SysCommand_ActionCancelResponse,
                new SysCommand_ActionCancelResponse
                {
                    action_name = actionName,
                    goal_id = goalId,
                    return_code = (int)RosActionCancelReturnCode.None,
                });
            UpdateConnection();
            Assert.AreEqual(mainThread, cancellationThread);
            Assert.IsTrue(cancellation.Result.Accepted);

            Enqueue(SysCommand.k_SysCommand_ActionResult,
                new SysCommand_ActionResult
                {
                    action_name = actionName,
                    goal_id = goalId,
                    status = (int)RosActionGoalStatus.Canceled,
                });
            EnqueuePayload(actionName, new ResultMsg { value = 99 });
            UpdateConnection();
            Assert.AreEqual(mainThread, resultThread);
            Assert.AreEqual(99, goal.Result.Result.Result.value);
            Assert.AreEqual(RosActionGoalStatus.Canceled, goal.Result.Result.Status);

            feedbackThread = -1;
            Enqueue(SysCommand.k_SysCommand_ActionFeedback,
                new SysCommand_ActionGoal { action_name = actionName, goal_id = goalId });
            EnqueuePayload(actionName, new FeedbackMsg { value = 100 });
            UpdateConnection();
            Assert.AreEqual(-1, feedbackThread, "Stale goal events must be ignored");
        }

        [Test]
        public void ConnectionLoss_OnlyFaultsPendingGoalWhenUpdateDrainsQueue()
        {
            int mainThread = Thread.CurrentThread.ManagedThreadId;
            string actionName = "/disconnect_action_" + Guid.NewGuid().ToString("N");
            var client = m_Connection.RegisterActionClient<GoalMsg, ResultMsg, FeedbackMsg>(
                actionName);
            var dispatch = (IRosActionClientInternal)client;
            dispatch.ResolveEndpointCapability(true);
            dispatch.OnRegistered();
            var acceptance = client.SendGoalAsync(new GoalMsg());
            string goalId = GetOnlyGoalId(client);
            dispatch.OnGoalResponse(new SysCommand_ActionGoalResponse
            {
                action_name = actionName,
                goal_id = goalId,
                accepted = true,
            });
            IRosActionGoal<ResultMsg> goal = acceptance.Result.Goal;
            int completionThread = -1;
            goal.Result.ContinueWith(
                _ => completionThread = Thread.CurrentThread.ManagedThreadId,
                TaskContinuationOptions.ExecuteSynchronously);

            MethodInfo connectionLost = typeof(ROSConnection).GetMethod(
                "OnConnectionLostCallback", BindingFlags.Instance | BindingFlags.NonPublic);
            var socketThread = new Thread(() => connectionLost.Invoke(m_Connection, null));
            socketThread.Start();
            socketThread.Join();

            Assert.IsFalse(goal.Result.IsCompleted,
                "The socket thread must only enqueue the disconnect event");
            UpdateConnection();
            Assert.IsTrue(goal.Result.IsFaulted);
            Assert.AreEqual(mainThread, completionThread);
            Assert.AreEqual(RosActionClientState.WaitingForConnection, client.State);
        }

        [Test]
        public void RegisterActionClient_RejectsMismatchedSubtopics()
        {
            MessageRegistry.Register(k_RosActionName, FeedbackMsg.Deserialize,
                MessageSubtopic.Result);
            Assert.Throws<ArgumentException>(() =>
                m_Connection.RegisterActionClient<GoalMsg, ResultMsg, FeedbackMsg>(
                    "/invalid_action_" + Guid.NewGuid().ToString("N")));
            MessageRegistry.Register(k_RosActionName, FeedbackMsg.Deserialize,
                MessageSubtopic.Feedback);
        }

        void Enqueue<T>(string command, T value)
        {
            m_Incoming.Enqueue(Tuple.Create(
                command, Encoding.UTF8.GetBytes(JsonUtility.ToJson(value))));
        }

        void EnqueuePayload(string actionName, Message message)
        {
            var serializer = new MessageSerializer();
            serializer.SerializeMessage(message);
            m_Incoming.Enqueue(Tuple.Create(actionName, serializer.GetBytes()));
        }

        void UpdateConnection()
        {
            m_Connection.SendMessage("Update");
        }

        static string GetOnlyGoalId(object client)
        {
            object goals = client.GetType()
                .GetField("m_Goals", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(client);
            foreach (object entry in (IEnumerable)goals)
                return (string)entry.GetType().GetProperty("Key").GetValue(entry);
            Assert.Fail("Expected one pending action goal");
            return null;
        }

        int OutgoingQueueCount()
        {
            object outgoing = typeof(ROSConnection)
                .GetField("m_OutgoingMessageQueue", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(m_Connection);
            object queue = outgoing.GetType()
                .GetField("m_OutgoingMessageQueue", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(outgoing);
            return (int)queue.GetType().GetProperty("Count").GetValue(queue);
        }
    }
}
