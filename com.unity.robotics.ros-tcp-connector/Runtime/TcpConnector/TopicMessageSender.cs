using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector
{
    // Wrapper class to store message with trailing pad flag
    internal class MessageWithTrailingPad
    {
        public Message Message { get; set; }
        public bool UseTrailingPad { get; set; }

        public MessageWithTrailingPad(Message message, bool useTrailingPad)
        {
            Message = message;
            UseTrailingPad = useTrailingPad;
        }
    }

    public class TopicMessageSender : OutgoingMessageSender
    {
        public string RosMessageName { get; private set; }

        public string TopicName { get; private set; }

        public int QueueSize { get; private set; }

        //Messages waiting to be sent queue.
        LinkedList<MessageWithTrailingPad> m_OutgoingMessages = new LinkedList<MessageWithTrailingPad>();

        //Keeps track of how many outgoing messages were removed due to queue overflow.
        //If a message is published but the queue is full, the counter is incremented, the
        //first message in the queue is recycled and the message to publish is put on the end of the queue.
        //If this is non 0, a SendTo call will decrement the counter instead of sending data.
        int m_QueueOverflowUnsentCounter = 0;

        //Latching - This message will be set if latching is enabled, and on reconnection, it will be sent again.
        Message m_LastMessageSent = null;

        //Optional, used if you want to pool messages and reuse them when they are no longer in use.
        IMessagePool m_MessagePool;
        public bool MessagePoolEnabled => m_MessagePool != null;

        public TopicMessageSender(string topicName, string rosMessageName, int queueSize)
        {
            if (queueSize < 1)
            {
                throw new Exception("Queue size must be greater than or equal to 1.");
            }

            TopicName = topicName;
            RosMessageName = rosMessageName;
            QueueSize = queueSize;
        }

        internal void Queue(Message message)
        {
            Queue(message, true);
        }

        internal void Queue(Message message, bool useTrailingPad)
        {
            lock (m_OutgoingMessages)
            {
                if (m_OutgoingMessages.Count >= QueueSize)
                {
                    //Remove outgoing messages that don't fit in the queue.
                    //Recycle the message if applicable
                    TryRecycleMessage(m_OutgoingMessages.First.Value.Message);
                    //Update the overflow counter.
                    m_QueueOverflowUnsentCounter++;
                    m_OutgoingMessages.RemoveFirst();
                }

                //Add a new valid message to the end with trailing pad flag
                var messageWithPad = new MessageWithTrailingPad(message, useTrailingPad);
                m_OutgoingMessages.AddLast(messageWithPad);
            }
        }

        SendToState GetMessageToSend(out Message messageToSend, out bool useTrailingPad)
        {
            SendToState result = SendToState.NoMessageToSendError;
            messageToSend = null;
            useTrailingPad = false;
            lock (m_OutgoingMessages)
            {
                if (m_QueueOverflowUnsentCounter > 0)
                {
                    //This means that we can't send message to ROS as fast as we're generating them.
                    //This could potentially be bad as it means that we are dropping messages!
                    m_QueueOverflowUnsentCounter--;
                    messageToSend = null;
                    result = SendToState.QueueFullWarning;
                }
                else if (m_OutgoingMessages.Count > 0)
                {
                    //Retrieve the next message and populate messageToSend.
                    var messageWithPad = m_OutgoingMessages.First.Value;
                    messageToSend = messageWithPad.Message;
                    useTrailingPad = messageWithPad.UseTrailingPad;
                    m_OutgoingMessages.RemoveFirst();
                    result = SendToState.Normal;
                }
            }

            return result;
        }

        SendToState GetMessageToSend(out Message messageToSend)
        {
            bool useTrailingPad;
            return GetMessageToSend(out messageToSend, out useTrailingPad);
        }

        public bool PeekNextMessageToSend(out Message messageToSend)
        {
            bool result = false;
            messageToSend = null;
            lock (m_OutgoingMessages)
            {
                if (m_OutgoingMessages.Count > 0)
                {
                    messageToSend = m_OutgoingMessages.First.Value.Message;
                    result = true;
                }
            }


            return result;
        }

        void SendMessageWithStream(MessageSerializer messageSerializer, Stream stream, Message message, bool useTrailingPad)
        {
            //Clear the serializer
            messageSerializer.Clear();
            //Set trailing pad flag if needed
            messageSerializer.AddTrailingPad4 = useTrailingPad;
            //Prepare the data to send.
            messageSerializer.Write(TopicName);
            messageSerializer.SerializeMessageWithLength(message);
            //Send via the stream.
            messageSerializer.SendTo(stream);
        }

        void SendMessageWithStream(MessageSerializer messageSerializer, Stream stream, Message message)
        {
            SendMessageWithStream(messageSerializer, stream, message, true);
        }

        public void PrepareLatchMessage()
        {
            if (m_LastMessageSent != null && !m_OutgoingMessages.Any())
            {
                //This topic is latching, so to mimic that functionality,
                // the last sent message is sent again with the new connection.
                m_OutgoingMessages.AddFirst(new MessageWithTrailingPad(m_LastMessageSent, true));
            }
        }

        public override SendToState SendInternal(MessageSerializer messageSerializer, Stream stream)
        {
            bool useTrailingPad;
            SendToState sendToState = GetMessageToSend(out Message toSend, out useTrailingPad);
            if (sendToState == SendToState.Normal)
            {
                SendMessageWithStream(messageSerializer, stream, toSend, useTrailingPad);

                //Recycle the message (if applicable).
                if (m_LastMessageSent != null && m_LastMessageSent != toSend)
                {
                    TryRecycleMessage(m_LastMessageSent);
                }

                m_LastMessageSent = toSend;
            }

            return sendToState;
        }

        public override void ClearAllQueuedData()
        {
            List<Message> toRecycle;
            lock (m_OutgoingMessages)
            {
                toRecycle = new List<Message>(m_OutgoingMessages.Select(mwp => mwp.Message));
                m_OutgoingMessages.Clear();
                m_QueueOverflowUnsentCounter = 0;
            }

            foreach (Message messageToRecycle in toRecycle)
            {
                TryRecycleMessage(messageToRecycle);
            }
        }

        void TryRecycleMessage(Message toRecycle)
        {
            if (m_MessagePool != null)
            {
                //Add the message back to the pool.
                m_MessagePool.AddMessage(toRecycle);
            }
        }

        public void SetMessagePool(IMessagePool messagePool)
        {
            m_MessagePool = messagePool;
        }
    }
}
