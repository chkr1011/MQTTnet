using System.Collections.Generic;
using MQTTnet.Protocol;

namespace MQTTnet.Server
{
    public class MqttSubscriptionInterceptorContext
    {
        public MqttSubscriptionInterceptorContext(string clientId, MqttTopicFilter topicFilter, IDictionary<object, object> sessionItems)
        {
            ClientId = clientId;
            TopicFilter = topicFilter;
            SessionItems = sessionItems;
            ResultCode = ConvertToSubscribeReasonCode(topicFilter.QualityOfServiceLevel);
        }

        public string ClientId { get; }

        public MqttTopicFilter TopicFilter { get; set; }

        /// <summary>
        /// Gets or sets a key/value collection that can be used to share data within the scope of this session.
        /// </summary>
        public IDictionary<object, object> SessionItems { get; }

        public bool AcceptSubscription
        {
            get => ResultCode < MqttSubscribeReasonCode.UnspecifiedError;
            /*  [Obsolete("Set error directly with ResultCode")] // Requires language 8.2 to have here. */
            set {
                if (!value && ResultCode < MqttSubscribeReasonCode.UnspecifiedError)
                {
                    ResultCode = MqttSubscribeReasonCode.UnspecifiedError;
                }
                else if (value && ResultCode >= MqttSubscribeReasonCode.UnspecifiedError) 
                {
                    ResultCode = MqttSubscribeReasonCode.GrantedQoS0;
                }
            }
        }

        public MqttSubscribeReasonCode ResultCode { get; set; }

        public bool CloseConnection { get; set; }

        public static MqttSubscribeReasonCode ConvertToSubscribeReasonCode(MqttQualityOfServiceLevel qualityOfServiceLevel)
        {
            switch (qualityOfServiceLevel)
            {
                case MqttQualityOfServiceLevel.AtMostOnce: return MqttSubscribeReasonCode.GrantedQoS0;
                case MqttQualityOfServiceLevel.AtLeastOnce: return MqttSubscribeReasonCode.GrantedQoS1;
                case MqttQualityOfServiceLevel.ExactlyOnce: return MqttSubscribeReasonCode.GrantedQoS2;
                default: return MqttSubscribeReasonCode.UnspecifiedError;
            }
        }
    }
}
