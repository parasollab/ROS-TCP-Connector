using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector.MessageGeneration
{
    public class MessageSerializer
    {
        // static data to insert into the serialization list, so that we don't have to alloc
        static readonly byte[] k_Ros2Header = new byte[] { 0, 1, 0, 0 };
        static readonly byte[] k_NullByte = new byte[] { 0 };
        static readonly byte[][] k_PaddingBytes = new byte[][]
        {
            null,
            new byte[]{ 0 },
            new byte[]{ 0, 0 },
            new byte[]{ 0, 0, 0 },
            new byte[]{ 0, 0, 0, 0 },
            new byte[]{ 0, 0, 0, 0, 0 },
            new byte[]{ 0, 0, 0, 0, 0, 0 },
            new byte[]{ 0, 0, 0, 0, 0, 0, 0 },
            new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0 },
        };

        int m_AlignmentOffset;
        int m_LengthCorrection; // alignment ignores the ros2 header etc, so add this to get actual length
        public int Length => m_AlignmentOffset + m_LengthCorrection;
        List<byte[]> m_ListOfSerializations = new List<byte[]>();
        
        // Add this flag; default true for legacy compatibility, set to false for CDR-minimal
        public bool AddTrailingPad4 { get; set; } = true;

        public MessageSerializer()
        {
        }

        public void Clear()
        {
            m_AlignmentOffset = 0;
            m_LengthCorrection = 0;
            m_ListOfSerializations.Clear();
        }

        public void SerializeMessageWithLength(Message message)
        {
            // insert a gap to put the length into
            int lengthIndex = m_ListOfSerializations.Count;
            m_ListOfSerializations.Add(null);
            m_LengthCorrection += 4;
            int preambleLength = Length;

            SerializeMessage(message);

            // fill in the gap, now that we know the length
            m_ListOfSerializations[lengthIndex] = BitConverter.GetBytes(Length - preambleLength);
        }

        public void SerializeMessage(Message message)
        {
#if ROS2
            // Insert the 4-byte ROS2 CDR header (Little-Endian flag + options)
            Write(k_Ros2Header);  // (Added) ensure CDR encapsulation header 0x00_01_00_00 is present
#endif
            m_LengthCorrection += m_AlignmentOffset;
            m_AlignmentOffset = 0; // header doesn't affect alignment of subsequent data
            message.SerializeTo(this);

#if ROS2
            if (AddTrailingPad4)
            {
                // Unconditional 4 bytes to match the "ROS" blob you posted
                m_ListOfSerializations.Add(k_PaddingBytes[4]);
                m_AlignmentOffset += 4;
            }
#endif
        }

        public byte[] GetBytes()
        {
            byte[] serializedMessage = new byte[Length];
            int writeIndex = 0;
            foreach (byte[] statement in m_ListOfSerializations)
            {
                if (statement == null)
                    continue;
                statement.CopyTo(serializedMessage, writeIndex);
                writeIndex += statement.Length;
            }
            return serializedMessage;
        }

        public List<byte[]> GetBytesSequence()
        {
            // TODO: check what's faster - copying the list...
            List<byte[]> result = new List<byte[]>(m_ListOfSerializations);
            // ...or giving away the old list and making a new one?
            //List<byte[]> result = m_ListOfSerializations;
            //m_ListOfSerializations = new List<byte[]>();
            return result;
        }

        public void SendTo(System.IO.Stream stream)
        {
            foreach (byte[] statement in m_ListOfSerializations)
                stream.Write(statement, 0, statement.Length);
        }

        // Alignment, offset, padding
        // https://github.com/eProsima/Fast-CDR/blob/53a0b8cae0b9083db69821be0edb97c944755591/include/fastcdr/Cdr.h#L239
        void Align(int dataSize)
        {
#if ROS2
            int padding = (dataSize - (m_AlignmentOffset % dataSize)) & (dataSize - 1);
            if (padding > 0)
                m_ListOfSerializations.Add(k_PaddingBytes[padding]);
            m_AlignmentOffset += padding;
#endif
        }

        public void Write(Message message)
        {
            message.SerializeTo(this);
        }

        public void Write(bool value)
        {
            // Write boolean as a single byte (0 or 1):contentReference[oaicite:3]{index=3}
            m_ListOfSerializations.Add(new byte[] { value ? (byte)1 : (byte)0 }); // (Changed) explicitly write 0x00 or 0x01
            m_AlignmentOffset += sizeof(bool);
        }

        public void Write(byte value)
        {
            m_ListOfSerializations.Add(new byte[] { value });
            m_AlignmentOffset += sizeof(byte);
        }

        public void Write(sbyte value)
        {
            m_ListOfSerializations.Add(new byte[] { (byte)value });
            m_AlignmentOffset += sizeof(sbyte);
        }

        public void Write(short value)
        {
            Align(sizeof(short));
            m_ListOfSerializations.Add(BitConverter.GetBytes(value));
            m_AlignmentOffset += sizeof(short);
        }

        public void Write(ushort value)
        {
            Align(sizeof(ushort));
            m_ListOfSerializations.Add(BitConverter.GetBytes(value));
            m_AlignmentOffset += sizeof(ushort);
        }

        public void Write(int value)
        {
            Align(sizeof(int));
            m_ListOfSerializations.Add(BitConverter.GetBytes(value));
            m_AlignmentOffset += sizeof(int);
        }

        public void Write(uint value)
        {
            Align(sizeof(uint));
            m_ListOfSerializations.Add(BitConverter.GetBytes(value));
            m_AlignmentOffset += sizeof(uint);
        }

        public void Write(long value)
        {
            Align(sizeof(long));
            m_ListOfSerializations.Add(BitConverter.GetBytes(value));
            m_AlignmentOffset += sizeof(long);
        }

        public void Write(ulong value)
        {
            Align(sizeof(ulong));
            m_ListOfSerializations.Add(BitConverter.GetBytes(value));
            m_AlignmentOffset += sizeof(ulong);
        }

        public void Write(float value)
        {
            Align(sizeof(float));
            m_ListOfSerializations.Add(BitConverter.GetBytes(value));
            m_AlignmentOffset += sizeof(float);
        }

        public void Write(double value)
        {
            Align(sizeof(double));
            m_ListOfSerializations.Add(BitConverter.GetBytes(value));
            m_AlignmentOffset += sizeof(double);
        }

        public void WriteLength<T>(T[] values)
        {
            // Write(values.Length);
            // // sequence length is 4-byte aligned
            // Align(sizeof(int));
            // Write(values.Length);

            // CDR: sequence length is 4-byte aligned and written once
            Align(sizeof(int));
            m_ListOfSerializations.Add(BitConverter.GetBytes(values.Length));
            m_AlignmentOffset += sizeof(int);
        }

        public void Write<T>(T[] values) where T : Message
        {
            if (values.Length == 0)
                return; // (Added comment) No elements, skip alignment/padding for empty sequence (esp. if final field)
// #if ROS2
//             // Align array elements to 8-byte boundary (max alignment needed for ROS2 message elements)
//             Align(8);  // (Added) ensure proper alignment for first element of complex message
// #endif
            foreach (T entry in values)
            {
                entry.SerializeTo(this);
            }
        }

        public void Write(bool[] values)
        {
            if (values.Length == 0)
                return; // (Added comment) Empty boolean array: length written separately, no content to align
            // Copy bool array as bytes (each bool is one byte 0/1 in ROS2)
            byte[] buffer = new byte[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                buffer[i] = values[i] ? (byte)1 : (byte)0;  // (Changed) ensure booleans are 0 or 1
            }
            m_ListOfSerializations.Add(buffer);
            m_AlignmentOffset += values.Length;
        }

        public void Write(byte[] values)
        {
            if (values.Length == 0)
                return;

            m_ListOfSerializations.Add(values);
            m_AlignmentOffset += values.Length;
        }

        public void Write(sbyte[] values)
        {
            if (values.Length == 0)
                return;

            byte[] buffer = new byte[values.Length];
            Buffer.BlockCopy(values, 0, buffer, 0, buffer.Length);
            m_ListOfSerializations.Add(buffer);
            m_AlignmentOffset += buffer.Length;
        }

        public void Write(short[] values)
        {
            if (values.Length == 0)
                return;

            Align(sizeof(short));
            byte[] buffer = new byte[values.Length * sizeof(short)];
            Buffer.BlockCopy(values, 0, buffer, 0, buffer.Length);
            m_ListOfSerializations.Add(buffer);
            m_AlignmentOffset += buffer.Length;
        }

        public void Write(ushort[] values)
        {
            if (values.Length == 0)
                return;

            Align(sizeof(ushort));
            byte[] buffer = new byte[values.Length * sizeof(ushort)];
            Buffer.BlockCopy(values, 0, buffer, 0, buffer.Length);
            m_ListOfSerializations.Add(buffer);
            m_AlignmentOffset += buffer.Length;
        }

        public void Write(int[] values)
        {
            if (values.Length == 0)
                return;

            Align(sizeof(int));
            byte[] buffer = new byte[values.Length * sizeof(int)];
            Buffer.BlockCopy(values, 0, buffer, 0, buffer.Length);
            m_ListOfSerializations.Add(buffer);
            m_AlignmentOffset += buffer.Length;
        }

        public void Write(uint[] values)
        {
            if (values.Length == 0)
                return;

            Align(sizeof(uint));
            byte[] buffer = new byte[values.Length * sizeof(uint)];
            Buffer.BlockCopy(values, 0, buffer, 0, buffer.Length);
            m_ListOfSerializations.Add(buffer);
            m_AlignmentOffset += buffer.Length;
        }

        public void Write(float[] values)
        {
            if (values.Length == 0)
                return;

            Align(sizeof(float));
            byte[] buffer = new byte[values.Length * sizeof(float)];
            Buffer.BlockCopy(values, 0, buffer, 0, buffer.Length);
            m_ListOfSerializations.Add(buffer);
            m_AlignmentOffset += buffer.Length;
        }

        public void Write(double[] values)
        {
            if (values.Length == 0)
                return;

            Align(sizeof(double));
            byte[] buffer = new byte[values.Length * sizeof(double)];
            Buffer.BlockCopy(values, 0, buffer, 0, buffer.Length);
            m_ListOfSerializations.Add(buffer);
            m_AlignmentOffset += buffer.Length;
        }

        public void Write(long[] values)
        {
            if (values.Length == 0)
                return;

            Align(sizeof(long));
            byte[] buffer = new byte[values.Length * sizeof(long)];
            Buffer.BlockCopy(values, 0, buffer, 0, buffer.Length);
            m_ListOfSerializations.Add(buffer);
            m_AlignmentOffset += buffer.Length;
        }

        public void Write(ulong[] values)
        {
            if (values.Length == 0)
                return;

            Align(sizeof(ulong));
            byte[] buffer = new byte[values.Length * sizeof(ulong)];
            Buffer.BlockCopy(values, 0, buffer, 0, buffer.Length);
            m_ListOfSerializations.Add(buffer);
            m_AlignmentOffset += buffer.Length;
        }

        public void Write(string inputString)
        {
            byte[] encodedString = Encoding.UTF8.GetBytes(inputString ?? string.Empty);
#if !ROS2
            // (ROS1) 4-byte length + string bytes (no null terminator)
            m_ListOfSerializations.Add(BitConverter.GetBytes(encodedString.Length));
            m_ListOfSerializations.Add(encodedString);
            m_AlignmentOffset += 4 + encodedString.Length;
#else
            // (ROS2) Align to 4-byte, then write length including null terminator, then string bytes + null
            Align(sizeof(int));  // 4-byte alignment for string length
            m_ListOfSerializations.Add(BitConverter.GetBytes(encodedString.Length + 1));
            m_ListOfSerializations.Add(encodedString);
            m_ListOfSerializations.Add(k_NullByte);
            m_AlignmentOffset += 4 + encodedString.Length + 1;
#endif
        }

        public void WriteUnaligned(string inputString)
        {
            byte[] encodedString = Encoding.UTF8.GetBytes(inputString);

#if !ROS2
            m_ListOfSerializations.Add(BitConverter.GetBytes(encodedString.Length));
            m_ListOfSerializations.Add(encodedString);

            m_AlignmentOffset += 4 + encodedString.Length;
#else
            // ROS2 strings are 4-byte aligned, and padded with a null byte at the end
            m_ListOfSerializations.Add(BitConverter.GetBytes(encodedString.Length + 1));
            m_ListOfSerializations.Add(encodedString);
            m_ListOfSerializations.Add(k_NullByte);

            m_AlignmentOffset += 4 + encodedString.Length + 1;
#endif
        }

        public void Write(string[] values)
        {
            foreach (string entry in values)
            {
                Write(entry);
            }
        }
    }
}
