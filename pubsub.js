"use strict";

module.exports = function(RED) {
  const { MessageTransport } = require('nodejs-helpers');
  const PubSub = require("@google-cloud/pubsub");

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

  function bufferToJson(data) {
    let _json = {};
    try {
      _json = JSON.parse(data ? data.toString() : '{}');
    } catch (err) {
      console.error(err);
    }

    return _json;
  }
  function GooglePubsubInNode(n) {
    RED.nodes.createNode(this, n);
    this.gcp_project_id = n.gcp_project_id;
    this.topic = n.topic;
    this.pubsub_topic = n.pubsub_topic;
    this.pubsub_subscription = n.pubsub_subscription;
    this.active = (n.active === null || typeof n.active === "undefined") || n.active;
    this.pubsubClient = PubSub({ projectId: this.gcp_project_id });
    this.subscription = this.pubsubClient.subscription(this.pubsub_subscription);
    const node = this;

    node.status({ fill: "blue", shape: "dot", text: "google-pubsub.status.waiting" });

    const messageHandler = function messageHandler(message) {
      node.error('HANDLING');
      node.status({ fill: "green", shape: "ring", text: "google-pubsub.status.receiving" });

      try {
        const msg = {
          payload: bufferToJson(message.data),
          id: message.id,
          connectionId: message.connectionId,
          ackId: message.ackId,
          attributes: message.attributes,
          received: message.received,
        };
        node.send(msg);

        node.status({ fill: "green", shape: "dot", text: "google-pubsub.status.received" });
        message.ack();
      } catch (err) {
        node.error(err);
        node.status({ fill: "red", shape: "ring", text: "google-pubsub.status.failed" });
      }
    };
    const messageErrorHandler = function messageErrorHandler(err) {
      node.error(err);
      node.status({ fill: "red", shape: "ring", text: "google-pubsub.status.failed" });
    };

    this.setState = function setState(active) {
      this.active = active;

      if (this.active) {
        this.subscription.on(`message`, messageHandler);
        this.subscription.on('error', messageErrorHandler);
        this.status({ fill: "blue", shape: "dot", text: "google-pubsub.status.waiting" });
      } else {
        this.subscription.removeListener('message', messageHandler);
        this.subscription.removeListener('error', messageErrorHandler);
        this.status({ fill: "grey", shape: "dot", text: "google-pubsub.status.disabled" });
      }
    };

    this.on("input", function(msg) {
      try {
        node.error('ACKING');
        node.subscription.ack_(msg);
      } catch (err) {
        node.error(err);
      }
    });
    this.setState(this.active);

    this.on('close', function(done) {
      node.setState(false);
      done();
    });
  }
  RED.nodes.registerType("google-pubsub in", GooglePubsubInNode);

  RED.httpAdmin.post("/google-pubsub/:id/:state", RED.auth.needsPermission("debug.write"), function(req, res) {
    var node = RED.nodes.getNode(req.params.id);
    var state = req.params.state;
    if (node !== null && typeof node !== "undefined") {
      if (state === "enable") {
        node.setState(true);
        res.sendStatus(200);
      } else if (state === "disable") {
        node.setState(false);
        res.sendStatus(201);
      } else {
        res.sendStatus(404);
      }
    } else {
      res.sendStatus(404);
    }
  });
};
