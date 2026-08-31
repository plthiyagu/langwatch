/**
 * Live Azure cost evidence script.
 *
 * Drives the SHIPPED Copilot Studio source end to end against a REAL Azure
 * subscription, so the verdict rests on today's bill rather than on a fixture:
 *
 *   1. Mint an org + team + IngestionSource of type `copilot_studio_dataverse`
 *      whose pullConfig carries a real `azureSubscriptionId`, with the app
 *      registration's secret encrypted exactly as the write path encrypts it.
 *   2. Invoke `runIngestionPull` — the same entry point the scheduler uses.
 *   3. Read `governance_cost_rollup_1d` from ClickHouse and print what landed.
 *
 * The secret is read from the environment and never printed.
 *
 * Usage:
 *   AZURE_SUBSCRIPTION_ID=... CS_TENANT_ID=... CS_CLIENT_ID=... \
 *   CS_CLIENT_SECRET=... CS_ENVIRONMENT_URL=... pnpm tsx <this file>
 */

import { createClient } from "@clickhouse/client";
import { randomBytes } from "crypto";
import { encryptParserConfigCredentials } from "../../../ee/governance/services/activity-monitor/ingestionCredentials";
import { ensureHiddenGovernanceProject } from "../../../ee/governance/services/governanceProject.service";
import { runIngestionPull } from "../../../ee/governance/services/pullers/pullerWorker";
import { prisma } from "../../../src/server/db";

const CLICKHOUSE_URL =
  process.env.CLICKHOUSE_URL ??
  "http://default:langwatch@localhost:8123/langwatch";

function rid(prefix: string) {
  return `${prefix}_${randomBytes(8).toString("hex")}`;
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

async function main(): Promise<void> {
  const subscriptionId = required("AZURE_SUBSCRIPTION_ID");
  const environmentUrl = required("CS_ENVIRONMENT_URL");
  const slug = `azure-cost-live-${Date.now()}`;

  console.log(`[azure-cost-live] namespace=${slug}`);
  console.log(`[azure-cost-live] subscription=${subscriptionId}`);
  console.log(`[azure-cost-live] environmentUrl=${environmentUrl}`);

  const org = await prisma.organization.create({
    data: { id: rid("org_azc"), name: "Azure Cost Live", slug },
  });
  const team = await prisma.team.create({
    data: {
      id: rid("team_azc"),
      name: "Azure Cost Live Team",
      slug: `${slug}-team`,
      organizationId: org.id,
    },
  });

  const parserConfig = encryptParserConfigCredentials({
    adapter: "copilot_studio_dataverse",
    environmentUrl,
    azureSubscriptionId: subscriptionId,
    schedule: "*/15 * * * *",
    credentials: {
      tenantId: required("CS_TENANT_ID"),
      clientId: required("CS_CLIENT_ID"),
      clientSecret: required("CS_CLIENT_SECRET"),
    },
  });

  const source = await prisma.ingestionSource.create({
    data: {
      organizationId: org.id,
      teamId: team.id,
      sourceType: "copilot_studio_dataverse",
      name: slug,
      ingestSecretHash: rid("hash"),
      status: "awaiting_first_event",
      pullSchedule: "*/15 * * * *",
      parserConfig: parserConfig as never,
    },
  });
  console.log(`[azure-cost-live] IngestionSource minted: id=${source.id}`);

  let outcome: Awaited<ReturnType<typeof runIngestionPull>> | null = null;
  try {
    outcome = await runIngestionPull({ sourceId: source.id, cursor: null });
    console.log(
      `[azure-cost-live] runIngestionPull outcome: events=${outcome.eventCount} cursor=${JSON.stringify(outcome.nextCursor)}`,
    );
  } catch (error) {
    console.error(
      `[azure-cost-live] runIngestionPull THREW: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  const govProject = await ensureHiddenGovernanceProject(prisma, org.id);
  console.log(`[azure-cost-live] governance project: ${govProject.id}`);

  await new Promise((resolve) => setTimeout(resolve, 3000));

  const ch = createClient({ url: CLICKHOUSE_URL });
  try {
    const rollup = await ch.query({
      query: `
        SELECT
          Day,
          CostSource,
          Provider,
          Model,
          CurrencyCode,
          AmountNanoMinor,
          AmountNanoUsd,
          ExactOrEstimate
        FROM governance_cost_rollup_1d FINAL
        WHERE TenantId = {tenantId:String}
        ORDER BY Day ASC, Model ASC
      `,
      query_params: { tenantId: govProject.id },
      format: "JSONEachRow",
    });
    const rows = (await rollup.json()) as Array<Record<string, unknown>>;
    console.log(
      `[azure-cost-live] governance_cost_rollup_1d rows: ${rows.length}`,
    );
    for (const row of rows) {
      console.log(`  ${JSON.stringify(row)}`);
    }

    const ocsf = await ch.query({
      query: `
        SELECT EventId, ActionName, TargetName, SourceType, toString(EventTime) AS EventTimeIso
        FROM governance_ocsf_events
        WHERE TenantId = {tenantId:String}
        ORDER BY EventTime ASC
        LIMIT 50
      `,
      query_params: { tenantId: govProject.id },
      format: "JSONEachRow",
    });
    const ocsfRows = (await ocsf.json()) as Array<Record<string, unknown>>;
    console.log(`[azure-cost-live] governance_ocsf_events rows: ${ocsfRows.length}`);
    for (const row of ocsfRows) {
      console.log(`  ${JSON.stringify(row)}`);
    }

    console.log(`[azure-cost-live] ORG_ID=${org.id}`);
    console.log(`[azure-cost-live] TENANT_ID=${govProject.id}`);
    console.log(`[azure-cost-live] SOURCE_ID=${source.id}`);
  } finally {
    await ch.close();
  }
}

main()
  .catch((error) => {
    console.error("[azure-cost-live] ERROR", error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
