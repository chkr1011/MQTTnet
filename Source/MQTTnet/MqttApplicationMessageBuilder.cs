﻿using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using MQTTnet.Exceptions;
using MQTTnet.Packets;
using MQTTnet.Protocol;

namespace MQTTnet
{
    public class MqttApplicationMessageBuilder
    {
        private readonly List<MqttUserProperty> _userProperties = new List<MqttUserProperty>();

        private MqttQualityOfServiceLevel _qualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce;
        private string _topic;
        private byte[] _payload;
        private bool _retain;
        private string _contentType;
        private string _responseTopic;
        private byte[] _correlationData;
        private ushort? _topicAlias;
        private uint? _subscriptionIdentifier;
        private uint? _messageExpiryInterval;
        private MqttPayloadFormatIndicator? _payloadFormatIndicator;

        public MqttApplicationMessageBuilder WithTopic(string topic)
        {
            _topic = topic;
            return this;
        }

        public MqttApplicationMessageBuilder WithPayload(IEnumerable<byte> payload)
        {
            if (payload == null)
            {
                _payload = null;
                return this;
            }

            _payload = payload.ToArray();
            return this;
        }

        public MqttApplicationMessageBuilder WithPayload(Stream payload)
        {
            return WithPayload(payload, payload.Length - payload.Position);
        }

        public MqttApplicationMessageBuilder WithPayload(Stream payload, long length)
        {
            if (payload == null)
            {
                _payload = null;
                return this;
            }

            if (payload.Length == 0)
            {
                _payload = new byte[0];
            }
            else
            {
                _payload = new byte[length];
                payload.Read(_payload, 0, _payload.Length);
            }

            return this;
        }

        public MqttApplicationMessageBuilder WithPayload(string payload)
        {
            if (payload == null)
            {
                _payload = null;
                return this;
            }

            _payload = string.IsNullOrEmpty(payload) ? new byte[0] : Encoding.UTF8.GetBytes(payload);
            return this;
        }

        public MqttApplicationMessageBuilder WithQualityOfServiceLevel(MqttQualityOfServiceLevel qualityOfServiceLevel)
        {
            _qualityOfServiceLevel = qualityOfServiceLevel;
            return this;
        }

        public MqttApplicationMessageBuilder WithRetainFlag(bool value = true)
        {
            _retain = value;
            return this;
        }

        public MqttApplicationMessageBuilder WithAtLeastOnceQoS()
        {
            _qualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce;
            return this;
        }

        public MqttApplicationMessageBuilder WithAtMostOnceQoS()
        {
            _qualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce;
            return this;
        }

        public MqttApplicationMessageBuilder WithExactlyOnceQoS()
        {
            _qualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce;
            return this;
        }

        /// <summary>
        /// This is only supported when using MQTTv5.
        /// </summary>
        public MqttApplicationMessageBuilder WithUserProperty(string name, string value)
        {
            _userProperties.Add(new MqttUserProperty(name, value));
            return this;
        }

        /// <summary>
        /// This is only supported when using MQTTv5.
        /// </summary>
        public MqttApplicationMessageBuilder WithContentType(string contentType)
        {
            _contentType = contentType;
            return this;
        }

        /// <summary>
        /// This is only supported when using MQTTv5.
        /// </summary>
        public MqttApplicationMessageBuilder WithResponseTopic(string responseTopic)
        {
            _responseTopic = responseTopic;
            return this;
        }

        /// <summary>
        /// This is only supported when using MQTTv5.
        /// </summary>
        public MqttApplicationMessageBuilder WithCorrelationData(byte[] correlationData)
        {
            _correlationData = correlationData;
            return this;
        }

        /// <summary>
        /// This is only supported when using MQTTv5.
        /// </summary>
        public MqttApplicationMessageBuilder WithTopicAlias(ushort topicAlias)
        {
            _topicAlias = topicAlias;
            return this;
        }

        /// <summary>
        /// This is only supported when using MQTTv5.
        /// </summary>
        public MqttApplicationMessageBuilder WithSubscriptionIdentifier(uint subscriptionIdentifier)
        {
            _subscriptionIdentifier = subscriptionIdentifier;
            return this;
        }

        /// <summary>
        /// This is only supported when using MQTTv5.
        /// </summary>
        public MqttApplicationMessageBuilder WithMessageExpiryInterval(uint messageExpiryInterval)
        {
            _messageExpiryInterval = messageExpiryInterval;
            return this;
        }

        /// <summary>
        /// This is only supported when using MQTTv5.
        /// </summary>
        public MqttApplicationMessageBuilder WithPayloadFormatIndicator(MqttPayloadFormatIndicator payloadFormatIndicator)
        {
            _payloadFormatIndicator = payloadFormatIndicator;
            return this;
        }

        public MqttApplicationMessage Build()
        {
            if (string.IsNullOrEmpty(_topic))
            {
                throw new MqttProtocolViolationException("Topic is not set.");
            }

            var applicationMessage = new MqttApplicationMessage
            {
                Topic = _topic,
                Payload = _payload ?? new byte[0],
                QualityOfServiceLevel = _qualityOfServiceLevel,
                Retain = _retain,
                ContentType = _contentType,
                ResponseTopic = _responseTopic,
                CorrelationData = _correlationData,
                TopicAlias = _topicAlias,
                SubscriptionIdentifier = _subscriptionIdentifier,
                MessageExpiryInterval = _messageExpiryInterval,
                PayloadFormatIndicator = _payloadFormatIndicator
            };

            applicationMessage.UserProperties = new List<MqttUserProperty>(_userProperties);

            return applicationMessage;
        }
    }
}
