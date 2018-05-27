﻿using Microsoft.AspNetCore.Connections;
using MQTTnet.Adapter;
using MQTTnet.Packets;
using MQTTnet.Serializer;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace MQTTnet.AspNetCore
{
    public class MqttConnectionContext : IMqttChannelAdapter
    {
        public IMqttPacketSerializer PacketSerializer { get; }
        public ConnectionContext Connection { get; }

        public string Endpoint => Connection.ConnectionId;

        public MqttConnectionContext(
            IMqttPacketSerializer packetSerializer,
            ConnectionContext connection)
        {
            PacketSerializer = packetSerializer;
            Connection = connection;
        }

        public Task ConnectAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task DisconnectAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            Connection.Transport.Input.Complete();
            Connection.Transport.Output.Complete();
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }

        public async Task<MqttBasePacket> ReceivePacketAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var input = Connection.Transport.Input;

            while (!cancellationToken.IsCancellationRequested)
            {
                ReadResult readResult;
                
                var readTask = input.ReadAsync(cancellationToken);
                if (readTask.IsCompleted)
                {
                    readResult = readTask.Result;
                }
                else
                {
                    readResult = await readTask;
                }

                var buffer = readResult.Buffer;

                var consumed = buffer.Start;
                var observed = buffer.Start;

                try
                {
                    if (!buffer.IsEmpty)
                    {
                        if (PacketSerializer.TryDeserialize(buffer, out var packet, out consumed, out observed))
                        {
                            return packet;
                        }
                    }
                    else if (readResult.IsCompleted)
                    {
                        break;
                    }
                }
                finally
                {
                    // The buffer was sliced up to where it was consumed, so we can just advance to the start.
                    // We mark examined as buffer.End so that if we didn't receive a full frame, we'll wait for more data
                    // before yielding the read again.
                    input.AdvanceTo(consumed, observed);
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            return null;
        }       

        public async Task SendPacketsAsync(TimeSpan timeout, IEnumerable<MqttBasePacket> packets, CancellationToken cancellationToken)
        {
            foreach (var packet in packets)
            {
                await WriteAsync(packet);
            }
        }

        public async Task WriteAsync(MqttBasePacket packet)
        {
            var buffer = PacketSerializer.Serialize(packet);
            await Connection.Transport.Output.WriteAsync(buffer.AsMemory());
        }

        private int messageId;
        public Task PublishAsync(MqttPublishPacket packet)
        {
            if (!packet.PacketIdentifier.HasValue && packet.QualityOfServiceLevel > MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce)
            {
                packet.PacketIdentifier = (ushort)Interlocked.Increment(ref messageId);
            }
            return WriteAsync(packet);
        }

        public Task SubscribeAsync(MqttSubscribePacket packet)
        {
            if (!packet.PacketIdentifier.HasValue)
            {
                packet.PacketIdentifier = (ushort)Interlocked.Increment(ref messageId);
            }
            return WriteAsync(packet);
        }

        public Task ConnectAsync(MqttConnectPacket packet)
        {
            if (string.IsNullOrEmpty(packet.ClientId))
            {
                packet.ClientId = Guid.NewGuid().ToString();
            }
            return WriteAsync(packet);
        }
    }
}