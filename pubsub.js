"use strict";

module.exports = function(RED) {
  const { MessageTransport } = require('nodejs-helpers');

  function GooglePubsubOutNode(n) {
    RED.nodes.createNode(this, n);
    this.topic = n.topic;
    this.pubsub_topic = n.pubsub_topic;
    const node = this;

    const pubsubClient = new MessageTransport(this.pubsub_topic);
    node.on("input", function(msg) {
      if (msg.hasOwnProperty("payload")) {
        node.status({ fill: "green", shape: "dot", text: "google-pubsub.status.publishing" });

        if (typeof msg.params === 'undefined') { msg.params = {}; }
        pubsubClient.publish(msg.payload, msg.params)
          .then(function(data) {
            node.status({ fill: "blue", shape: "dot", text: "google-pubsub.status.waiting" });
          })
          .catch(function(err) {
            node.status({ fill: "red", shape: "ring", text: "google-pubsub.status.failed" });
            node.error(err, msg);
          });
      } else { node.warn(RED._("google-pubsub.errors.nopayload")); }
    });
  }
  RED.nodes.registerType("google-pubsub out", GooglePubsubOutNode);
};
