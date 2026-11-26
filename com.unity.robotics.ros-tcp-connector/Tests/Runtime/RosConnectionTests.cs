using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using RosMessageTypes.BuiltinInterfaces;
using RosMessageTypes.Geometry;
using RosMessageTypes.Tf2;
using Unity.Robotics.ROSTCPConnector;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using Unity.Robotics.ROSTCPConnector.ROSGeometry;
using UnityEngine;
using UnityEngine.TestTools;

namespace UnitTests
{
    public class RosConnectionTests
    {
        [Test]
        public void GetOrCreateInstance_CallOnce_ReturnsValidInstance()
        {
            ROSConnection ros = ROSConnection.GetOrCreateInstance();
            ros.ConnectOnStart = false;
            Assert.NotNull(ros);
        }

        [Test]
        public void GetOrCreateInstance_CallTwice_ReturnsSameInstance()
        {
            ROSConnection ros = ROSConnection.GetOrCreateInstance();
            Assert.NotNull(ros);
            ros.ConnectOnStart = false;
            ROSConnection ros2 = ROSConnection.GetOrCreateInstance();
            Assert.AreEqual(ros, ros2);
        }

        // Changing the address while an earlier Connect() is still trying its own: the thread of
        // the earlier one used to live on, and each time it gave up on an attempt it cleared the
        // outgoing queue and marked every topic as unregistered, on the connection that had
        // replaced it. Loopback only; nothing here needs a ROS endpoint.
        [UnityTest]
        public IEnumerator Connect_WhileAnEarlierConnectIsStillTrying_LeavesTheNewConnectionAlone()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int listening = ((IPEndPoint)listener.LocalEndpoint).Port;
            // A port nothing listens on: taken from the system, then given back.
            var closed = new TcpListener(IPAddress.Loopback, 0);
            closed.Start();
            int refusing = ((IPEndPoint)closed.LocalEndpoint).Port;
            closed.Stop();

            var go = new GameObject("ReconnectTest");
            go.SetActive(false);
            ROSConnection ros = go.AddComponent<ROSConnection>();
            ros.ConnectOnStart = false;
            ros.ShowHud = false;
            ros.listenForTFMessages = false;
            go.SetActive(true);
            TcpClient accepted = null;
            try
            {
                string id = Guid.NewGuid().ToString("N");
                string published = "/reconnect_pub_" + id, subscribed = "/reconnect_sub_" + id;
                ros.RegisterPublisher<TimeMsg>(published);
                ros.Subscribe<TimeMsg>(subscribed, _ => { });

                ros.Connect("127.0.0.1", refusing);
                // Long enough for the first attempt to be refused and the thread to be waiting
                // for its next one.
                yield return new WaitForSecondsRealtime(0.3f);
                Assert.IsTrue(ros.HasConnectionError, "Nothing listens on the first address.");

                ros.Connect("127.0.0.1", listening);
                Assert.IsTrue(ros.HasConnectionThread);
                float deadline = Time.realtimeSinceStartup + 5f;
                while (!listener.Pending() && Time.realtimeSinceStartup < deadline) yield return null;
                Assert.IsTrue(listener.Pending(), "The second Connect() reaches the new address.");
                accepted = listener.AcceptTcpClient();

                RosTopicState pub = ros.GetTopic(published), sub = ros.GetTopic(subscribed);
                while (!(pub.SentPublisherRegistration && sub.SentSubscriberRegistration) &&
                       Time.realtimeSinceStartup < deadline) yield return null;
                Assert.IsTrue(pub.SentPublisherRegistration, "The publisher is announced on the new connection.");
                Assert.IsTrue(sub.SentSubscriberRegistration, "The subscriber is announced on the new connection.");

                // The first thread retried once a second: two of its rounds.
                yield return new WaitForSecondsRealtime(2.2f);
                Assert.IsTrue(pub.SentPublisherRegistration, "The thread of the first Connect() must not reset the publisher.");
                Assert.IsTrue(sub.SentSubscriberRegistration, "The thread of the first Connect() must not reset the subscriber.");
                Assert.IsFalse(listener.Pending(), "One connection to the new address, not two.");

                ros.Disconnect();
                Assert.IsFalse(ros.HasConnectionThread);
                Assert.IsTrue(ros.HasConnectionError, "A connection that was closed is not a working one.");
                yield return null;
                Assert.IsFalse(pub.SentPublisherRegistration, "Disconnect() ends the registrations, for the next Connect() to send again.");
            }
            finally
            {
                ros.Disconnect();
                accepted?.Close();
                listener.Stop();
                UnityEngine.Object.Destroy(go);
            }
        }
    }
}
