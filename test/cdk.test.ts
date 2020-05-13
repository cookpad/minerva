import {
  expect as expectCDK,
  matchTemplate,
  MatchStyle,
} from "@aws-cdk/assert";
import * as cdk from "@aws-cdk/core";
import Minerva = require("../lib/minerva-stack");

test("Empty Stack", () => {
  const app = new cdk.App();
  // WHEN
  /*
  const stack = new Minerva.MinervaStack(app, "MyTestStack");
  // THEN
  expectCDK(stack).to(
    matchTemplate(
      {
        Resources: {},
      },
      MatchStyle.EXACT
    )
  );
  */
});
