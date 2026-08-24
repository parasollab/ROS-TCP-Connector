using System.IO;
using JetBrains.Annotations;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector
{
    public abstract class SysCommand
    {
        public const string k_SysCommand_Handshake = "__handshake";
        public const string k_SysCommand_Log = "__log";
        public const string k_SysCommand_Warning = "__warn";
        public const string k_SysCommand_Error = "__error";
        public const string k_SysCommand_ServiceRequest = "__request";
        public const string k_SysCommand_ServiceResponse = "__response";
        public const string k_SysCommand_Subscribe = "__subscribe";
        public const string k_SysCommand_Publish = "__publish";
        public const string k_SysCommand_RosService = "__ros_service";
        public const string k_SysCommand_UnityService = "__unity_service";
        public const string k_SysCommand_TopicList = "__topic_list";
        public const string k_SysCommand_RemoveSubscriber = "__remove_subscriber";
        public const string k_SysCommand_RemovePublisher = "__remove_publisher";
        public const string k_SysCommand_RemoveRosService = "__remove_ros_service";
        public const string k_SysCommand_RemoveUnityService = "__remove_unity_service";
        public const string k_SysCommand_RosAction = "__ros_action";
        public const string k_SysCommand_RemoveRosAction = "__remove_ros_action";
        public const string k_SysCommand_ActionGoal = "__action_goal";
        public const string k_SysCommand_ActionCancel = "__action_cancel";
        public const string k_SysCommand_ActionRegistered = "__action_registered";
        public const string k_SysCommand_ActionGoalResponse = "__action_goal_response";
        public const string k_SysCommand_ActionFeedback = "__action_feedback";
        public const string k_SysCommand_ActionResult = "__action_result";
        public const string k_SysCommand_ActionCancelResponse = "__action_cancel_response";
        public const string k_SysCommand_ActionError = "__action_error";
        internal const string k_InternalConnectionStarted = "__internal_connection_started";
        internal const string k_InternalConnectionLost = "__internal_connection_lost";

        public abstract string Command
        {
            get;
        }

        public virtual object BuildParam()
        {
            return this;

        }
        public void PopulateSysCommand(MessageSerializer messageSerializer)
        {
            messageSerializer.Clear();
            // syscommands are sent as:
            // 4 byte command length, followed by that many bytes of the command
            // (all command names start with __ to distinguish them from ros topics)
            messageSerializer.Write(Command);
            // 4-byte json length, followed by a json string of that length
            string json = JsonUtility.ToJson(BuildParam());
            messageSerializer.WriteUnaligned(json);
        }

        public void SendTo([NotNull] Stream stream, MessageSerializer messageSerializer = null)
        {
            if (messageSerializer == null)
            {
                messageSerializer = new MessageSerializer();
            }

            PopulateSysCommand(messageSerializer);
            messageSerializer.SendTo(stream);
        }
    }

    public struct SysCommand_Topic
    {
        public string topic;
    }

    public struct SysCommand_TopicAndType
    {
        public string topic;
        public string message_name;
    }

    // For backwards compatibility, we encode the handshake in two stages:
    // Stage 1 - which must NEVER change - is just a version string and a metadata string.
    public struct SysCommand_Handshake
    {
        public string version;
        public string metadata;
    }

    // Stage 2 is the json encoded contents of the metadata string.
    // Because this structure may change with future versions of ROS TCP Connector, we only decode it
    // after checking the version number is correct.
    public struct SysCommand_Handshake_Metadata
    {
        public string protocol; // "ROS1" or "ROS2"
        public string[] capabilities;
    }

    public struct SysCommand_Log
    {
        public string text;
    }

    public struct SysCommand_Service
    {
        public int srv_id;
    }

    public struct SysCommand_TopicsRequest
    {
    }

    public struct SysCommand_TopicsResponse
    {
        public string[] topics;
        public string[] types;
    }

    public struct SysCommand_PublisherRegistration
    {
        public string topic;
        public string message_name;
        public int queue_size;
        public bool latch;
    }

    public struct SysCommand_ActionRegistration
    {
        public string action_name;
        public string message_name;
    }

    public struct SysCommand_ActionGoal
    {
        public string action_name;
        public string goal_id;
    }

    public struct SysCommand_ActionGoalResponse
    {
        public string action_name;
        public string goal_id;
        public bool accepted;
        public string ros_goal_id;
    }

    public struct SysCommand_ActionResult
    {
        public string action_name;
        public string goal_id;
        public int status;
    }

    public struct SysCommand_ActionCancelResponse
    {
        public string action_name;
        public string goal_id;
        public int return_code;
    }

    public struct SysCommand_ActionError
    {
        public string action_name;
        public string goal_id;
        public string code;
        public string message;
    }
}
