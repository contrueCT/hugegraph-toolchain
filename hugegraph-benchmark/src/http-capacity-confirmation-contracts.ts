/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import { z } from "zod";
import { httpCapacityPolicyV1Schema } from "./http-capacity-contracts.js";

const file = z
    .string()
    .min(1)
    .max(4096)
    .regex(/^[^\r\n\0]+$/);
export const httpCapacityConfirmationRequestV1Schema = z.strictObject({
    schemaVersion: z.literal(1),
    kind: z.literal("http-capacity-confirmation-request"),
    id: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$/),
    exploration: z.strictObject({ plan: file, executionSet: file, report: file }),
    project: file,
    sourceReview: z.strictObject({
        maxMilliseconds: z.number().int().min(1000).max(86400000),
        maxBytes: z
            .number()
            .int()
            .min(1024 ** 2)
            .max(8 * 1024 ** 3),
    }),
    budgets: z.strictObject({
        point: httpCapacityPolicyV1Schema.shape.pointBudgets,
        controller: httpCapacityPolicyV1Schema.shape.controllerBudgets,
        study: httpCapacityPolicyV1Schema.shape.studyBudgets,
    }),
});
export const httpCapacityConfirmationRequestV2Schema = httpCapacityConfirmationRequestV1Schema.extend({
    schemaVersion: z.literal(2),
    budgetMode: z.enum(["total", "stage-sum"]).default("total"),
});
export const httpCapacityConfirmationRequestSchema = z.discriminatedUnion("schemaVersion", [
    httpCapacityConfirmationRequestV1Schema,
    httpCapacityConfirmationRequestV2Schema,
]);
export type HttpCapacityConfirmationRequest = z.infer<typeof httpCapacityConfirmationRequestSchema>;
