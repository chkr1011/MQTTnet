using MQTTnet.Client.Connecting;
using MQTTnet.Client.Disconnecting;
using MQTTnet.Client.ExtendedAuthenticationExchange;
using MQTTnet.Client.Options;
using MQTTnet.Client.Publishing;
using MQTTnet.Client.Receiving;
using MQTTnet.Client.Subscribing;
using MQTTnet.Client.Unsubscribing;
using MQTTnet.Protocol;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MQTTnet.Client
{
    public static class MqttClientExtensions
    {
        public static IMqttClient UseConnectedHandler(this IMqttClient client, Func<MqttClientConnectedEventArgs, Task> handler)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            if (handler == null)
            {
                return client.UseConnectedHandler((IMqttClientConnectedHandler)null);
            }

            return client.UseConnectedHandler(new MqttClientConnectedHandlerDelegate(handler));
        }

        public static IMqttClient UseConnectedHandler(this IMqttClient client, Action<MqttClientConnectedEventArgs> handler)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            if (handler == null)
            {
                return client.UseConnectedHandler((IMqttClientConnectedHandler)null);
            }

            return client.UseConnectedHandler(new MqttClientConnectedHandlerDelegate(handler));
        }

        public static IMqttClient UseConnectedHandler(this IMqttClient client, IMqttClientConnectedHandler handler)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            client.ConnectedHandler = handler;
            return client;
        }

        public static IMqttClient UseDisconnectedHandler(this IMqttClient client, Func<MqttClientDisconnectedEventArgs, Task> handler)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            if (handler == null)
            {
                return client.UseDisconnectedHandler((IMqttClientDisconnectedHandler)null);
            }

            return client.UseDisconnectedHandler(new MqttClientDisconnectedHandlerDelegate(handler));
        }

        public static IMqttClient UseDisconnectedHandler(this IMqttClient client, Action<MqttClientDisconnectedEventArgs> handler)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            if (handler == null)
            {
                return client.UseDisconnectedHandler((IMqttClientDisconnectedHandler)null);
            }

            return client.UseDisconnectedHandler(new MqttClientDisconnectedHandlerDelegate(handler));
        }

        public static IMqttClient UseDisconnectedHandler(this IMqttClient client, IMqttClientDisconnectedHandler handler)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            client.DisconnectedHandler = handler;
            return client;
        }

        public static IMqttClient UseApplicationMessageReceivedHandler(this IMqttClient client, Func<MqttApplicationMessageReceivedEventArgs, Task> handler)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            if (handler == null)
            {
                return client.UseApplicationMessageReceivedHandler((IMqttApplicationMessageReceivedHandler)null);
            }

            return client.UseApplicationMessageReceivedHandler(new MqttApplicationMessageReceivedHandlerDelegate(handler));
        }

        public static IMqttClient UseApplicationMessageReceivedHandler(this IMqttClient client, Action<MqttApplicationMessageReceivedEventArgs> handler)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            if (handler == null)
            {
                return client.UseApplicationMessageReceivedHandler((IMqttApplicationMessageReceivedHandler)null);
            }

            return client.UseApplicationMessageReceivedHandler(new MqttApplicationMessageReceivedHandlerDelegate(handler));
        }

        public static IMqttClient UseApplicationMessageReceivedHandler(this IMqttClient client, IMqttApplicationMessageReceivedHandler handler)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            client.ApplicationMessageReceivedHandler = handler;
            return client;
        }

        public static Task<MqttClientAuthenticateResult> ReconnectAsync(this IMqttClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            if (client.Options == null)
            {
                throw new InvalidOperationException("_ReconnectAsync_ can be used only if _ConnectAsync_ was called before.");
            }

            return client.ConnectAsync(client.Options);
        }

        public static Task<MqttClientAuthenticateResult> ReconnectAsync(this IMqttClient client, CancellationToken cancellationToken)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            if (client.Options == null)
            {
                throw new InvalidOperationException("_ReconnectAsync_ can be used only if _ConnectAsync_ was called before.");
            }

            return client.ConnectAsync(client.Options, cancellationToken);
        }

        public static Task DisconnectAsync(this IMqttClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            return client.DisconnectAsync(CancellationToken.None);
        }

        public static Task DisconnectAsync(this IMqttClient client, MqttClientDisconnectOptions options)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            return client.DisconnectAsync(options, CancellationToken.None);
        }

        public static Task DisconnectAsync(this IMqttClient client, CancellationToken cancellationToken)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            return client.DisconnectAsync(new MqttClientDisconnectOptions(), cancellationToken);
        }

        public static Task<MqttClientSubscribeResult> SubscribeAsync(this IMqttClient client, params MqttTopicFilter[] topicFilters)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (topicFilters == null) throw new ArgumentNullException(nameof(topicFilters));

            var options = new MqttClientSubscribeOptions();
            options.TopicFilters.AddRange(topicFilters);

            return client.SubscribeAsync(options);
        }

        public static Task<MqttClientSubscribeResult> SubscribeAsync(this IMqttClient client, string topic, MqttQualityOfServiceLevel qualityOfServiceLevel)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (topic == null) throw new ArgumentNullException(nameof(topic));

            return client.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic(topic).WithQualityOfServiceLevel(qualityOfServiceLevel).Build());
        }

        public static Task<MqttClientSubscribeResult> SubscribeAsync(this IMqttClient client, string topic)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (topic == null) throw new ArgumentNullException(nameof(topic));

            return client.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic(topic).Build());
        }

        public static Task<MqttClientUnsubscribeResult> UnsubscribeAsync(this IMqttClient client, params string[] topicFilters)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (topicFilters == null) throw new ArgumentNullException(nameof(topicFilters));

            var options = new MqttClientUnsubscribeOptions();
            options.TopicFilters.AddRange(topicFilters);

            return client.UnsubscribeAsync(options);
        }

        public static Task<MqttClientAuthenticateResult> ConnectAsync(this IMqttClient client, IMqttClientOptions options)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            return client.ConnectAsync(options, CancellationToken.None);
        }

        public static Task SendExtendedAuthenticationExchangeDataAsync(this IMqttClient client, MqttExtendedAuthenticationExchangeData data)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            return client.SendExtendedAuthenticationExchangeDataAsync(data, CancellationToken.None);
        }

        public static Task<MqttClientSubscribeResult> SubscribeAsync(this IMqttClient client, MqttClientSubscribeOptions options)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            return client.SubscribeAsync(options, CancellationToken.None);
        }

        public static Task<MqttClientUnsubscribeResult> UnsubscribeAsync(this IMqttClient client, MqttClientUnsubscribeOptions options)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            return client.UnsubscribeAsync(options, CancellationToken.None);
        }

        public static Task<MqttClientPublishResult> PublishAsync(this IMqttClient client, MqttApplicationMessage applicationMessage)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (applicationMessage == null) throw new ArgumentNullException(nameof(applicationMessage));

            return client.PublishAsync(applicationMessage, CancellationToken.None);
        }

        public static async Task PublishAsync(this IMqttClient client, IEnumerable<MqttApplicationMessage> applicationMessages)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (applicationMessages == null) throw new ArgumentNullException(nameof(applicationMessages));

            foreach (var applicationMessage in applicationMessages)
            {
                await client.PublishAsync(applicationMessage).ConfigureAwait(false);
            }
        }

        public static Task<MqttClientPublishResult> PublishAsync(this IMqttClient client, string topic)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (topic == null) throw new ArgumentNullException(nameof(topic));

            return client.PublishAsync(new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .Build());
        }

        public static Task<MqttClientPublishResult> PublishAsync(this IMqttClient client, string topic, IEnumerable<byte> payload)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (topic == null) throw new ArgumentNullException(nameof(topic));

            return client.PublishAsync(new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .Build());
        }

        public static Task<MqttClientPublishResult> PublishAsync(this IMqttClient client, string topic, string payload)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (topic == null) throw new ArgumentNullException(nameof(topic));

            return client.PublishAsync(new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .Build());
        }

        public static Task<MqttClientPublishResult> PublishAsync(this IMqttClient client, string topic, string payload, MqttQualityOfServiceLevel qualityOfServiceLevel)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (topic == null) throw new ArgumentNullException(nameof(topic));

            return client.PublishAsync(new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel(qualityOfServiceLevel)
                .Build());
        }

        public static Task<MqttClientPublishResult> PublishAsync(this IMqttClient client, string topic, string payload, MqttQualityOfServiceLevel qualityOfServiceLevel, bool retain)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (topic == null) throw new ArgumentNullException(nameof(topic));

            return client.PublishAsync(new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel(qualityOfServiceLevel)
                .WithRetainFlag(retain)
                .Build());
        }

        public static Task<MqttClientPublishResult> PublishAsync(this IMqttClient client, string topic, string payload, bool retain)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (topic == null) throw new ArgumentNullException(nameof(topic));

            return client.PublishAsync(new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithRetainFlag(retain)
                .Build());
        }
    }
}