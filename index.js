require("dotenv").config();
const { initializeApp } = require("firebase-admin/app");
const admin = require("firebase-admin");
const { startStream, types } = require("near-lake-framework");
const JSON5 = require("json5");
const lakeConfig = {
  s3BucketName: "near-lake-data-mainnet",
  s3RegionName: "eu-central-1",
  startBlockHeight: 99097410,
};
const serviceAccount = require("./your-firebase-adminsdk.json");
const express = require("express");
const app = express();

initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const kSubscriptions = "subscriptions_channels";

function mapOutcomeToNearSocialNotifyObject(inputObject) {
  let transformedObject = {};
  if (
    inputObject.executionOutcome &&
    inputObject.executionOutcome.outcome.receiptIds &&
    Array.isArray(inputObject.executionOutcome.outcome.receiptIds)
  ) {
    inputObject.executionOutcome.outcome.receiptIds.forEach((_) => {
      const action = inputObject.receipt.receipt.Action.actions.find(
        (action) => action.FunctionCall
      );

      if (action) {
        const parsedAction = action.FunctionCall;
        const args = Buffer.from(parsedAction.args, "base64").toString("utf-8");

        let parsedJson = parseArgs(args);
        let notificationObj = getNotifyObjectFromArgs(parsedJson);
        let targetAccountId = notificationObj?.notification?.key;
        let notifyMessage = generateNotificationMessage(notificationObj);
        if (notifyMessage) {
          console.log(notifyMessage);
        }
        transformedObject = {
          targetAccountId: targetAccountId,
          methodName: parsedAction.methodName,
          notification: notifyMessage,
        };
      }
    });
  }

  return transformedObject;
}

function generateNotificationMessage(notificationObj) {
  if (
    !notificationObj?.notification?.value &&
    !notificationObj?.executor &&
    !notificationObj?.notification?.key &&
    !notificationObj?.notification?.value?.type
  ) {
    return;
  }

  const value = notificationObj.notification.value;
  const executorAccountId = notificationObj.executor;
  const targetAccountId = notificationObj.notification.key;

  let title = value?.type?.charAt(0).toUpperCase() + value?.type?.slice(1);

  let body;

  if (!value) {
    return;
  }
  // console.log(JSON.stringify(value));

  if (value.type === "follow" || value.type === "unfollow") {
    body = `${executorAccountId} ${value.type} ${targetAccountId}`;
  } else if (value.type === "poke") {
    body = `${executorAccountId} poke ${targetAccountId}`;
  } else if (value.type === "like") {
    body = `${executorAccountId} like`;

    if (value.item.path === `${targetAccountId}/post/main`) {
      body += ` ${targetAccountId} post`;
    } else if (value.item.path === `${targetAccountId}/post/comment`) {
      body += ` ${targetAccountId} comment`;
    } else if (value.item.path === `${targetAccountId}/post/insta`) {
      body += ` ${targetAccountId} insta`;
    } else {
      const pathParts = value.item.path.split("/");
      body += ` ${pathParts[0]} ${pathParts[1]}`;
    }
  } else if (value.type === "comment") {
    body = `${executorAccountId} replied to ${targetAccountId}`;

    if (
      value.item.path === `${targetAccountId}/post/main` ||
      value.item.path.split("/")[1] === "post"
    ) {
      body += ` post`;
    } else {
      body += ` ???`;
    }
  } else if (value.type && value.type.startsWith("devgovgigs/")) {
    if (value.type === "like") {
      body = `Liked ${targetAccountId}`;
    } else if (value.type === "reply") {
      body = `Replied to ${targetAccountId}`;
    } else if (value.type === "edit") {
      body = `Edited ${targetAccountId}`;
    } else if (value.type === "mention") {
      body = `Mentioned ${targetAccountId} in their DevHub post`;
    } else {
      body += ` ???`;
    }
  } else if (value.type === "mention") {
    body = `${executorAccountId} mentioned ${targetAccountId} in their`;

    if (value.item.path === `${targetAccountId}/post/main`) {
      body += ` post`;
    } else if (value.item.path === `${targetAccountId}/post/comment`) {
      body += ` comment`;
    } else {
      body += ` ???`;
    }
  } else if (value.type === "repost") {
    body = `${executorAccountId} reposted ${targetAccountId} in their`;

    if (value.item.path === `${targetAccountId}/post/main`) {
      body += ` post`;
    } else {
      body += ` ???`;
    }
  } else {
    body = "Near Social targetId Notification";
  }

  if (title && body) {
    return {
      title: "",
      body: body,
    };
  }
}

function getNotifyObjectFromArgs(parsedJson) {
  let notificationObj;
  try {
    if (parsedJson.data) {
      for (const accountId in parsedJson.data) {
        if (
          parsedJson.data[accountId].index &&
          parsedJson.data[accountId].index.notify
        ) {
          const notify = JSON5.parse(parsedJson.data[accountId].index.notify);
          if (accountId != notify.key) {
            notificationObj = {
              executor: accountId,
              notification: notify,
            };
          }
        }
      }
    }
  } catch (error) {}
  return notificationObj;
}

function parseArgs(args) {
  let parsedJson = {};
  try {
    parsedJson = JSON5.parse(args);
  } catch (e) {
    // console.log("Parsing Problem");
  }
  return parsedJson;
}

async function handleStreamerMessage(streamerMessage) {
  const relevantOutcomes = streamerMessage.shards
    .flatMap((shard) => shard.receiptExecutionOutcomes)
    .map((outcome) => ({
      receipt: {
        id: outcome.receipt.receiptId,
        receiverId: outcome.receipt.receiverId,
      },
      nearSocialMethodCallData: mapOutcomeToNearSocialNotifyObject(outcome),
    }))
    .filter((relevantOutcome) => {
      return (
        relevantOutcome.receipt.receiverId == "social.near" &&
        relevantOutcome.nearSocialMethodCallData.notification
      );
    });

  relevantOutcomes.forEach(async function (relevantOutcome) {
    const targetAccountId =
      relevantOutcome.nearSocialMethodCallData.targetAccountId;
    const notificationTitle =
      relevantOutcome.nearSocialMethodCallData.notification.title;
    const notificationBody =
      relevantOutcome.nearSocialMethodCallData.notification.body;
    // console.log(
    //   JSON.stringify(relevantOutcome.nearSocialMethodCallData.notification)
    // );
    const subscribers =
      (await getSubscribersBuyAccountID(targetAccountId)) ?? {};
    const fcmTokensList = Object.values(subscribers).filter(
      (token) => token !== null && token !== undefined && token !== ""
    );
    const imageUrl = "https://near.social/assets/logo.png";
    if (fcmTokensList.length > 0) {
      sendNotifications(
        fcmTokensList,
        notificationTitle,
        notificationBody,
        imageUrl
      );
    }
  });
}

async function sendNotifications(
  tokens,
  title,
  body,
  imageUrl,
  initialPageName,
  parameterData,
  sound
) {
  const admin = require("firebase-admin");

  const tokensArr = Array.from(tokens);
  const messageBatches = [];

  for (let i = 0; i < tokensArr.length; i += 500) {
    const tokensBatch = tokensArr.slice(i, Math.min(i + 500, tokensArr.length));
    const messages = {
      notification: {
        title,
        body,
        ...(imageUrl && { imageUrl: imageUrl }),
      },
      //   data: {
      //     initialPageName,
      //     parameterData,
      //   },
      android: {
        notification: {
          sound: "default",
        },
      },
      apns: {
        payload: {
          aps: {
            sound: "default",
          },
        },
      },
      tokens: tokensBatch,
    };
    messageBatches.push(messages);
  }

  let numSent = 0;

  await Promise.all(
    messageBatches.map(async (messages) => {
      const response = await admin.messaging().sendEachForMulticast(messages);
      numSent += response.successCount;
    })
  );

  return numSent;
}

async function getSubscribersBuyAccountID(accountId) {
  const firestore = admin.firestore();
  const accountIdChannel = await firestore
    .collection(kSubscriptions)
    .doc(accountId ?? "root.near")
    .get();

  const subscribers = accountIdChannel.data();

  return subscribers;
}

(async () => {
  await startStream(lakeConfig, handleStreamerMessage);
})();

app.get("/", function (req, res) {
  res.send("BOS Notification API");
});

app.listen(3000);
