import { Client } from "pg";

export type EnvDb = {
  HYPERDRIVE: { connectionString: string };
};

export async function withDb<T>(env: EnvDb, fn: (c: Client) => Promise<T>) {
  const client = new Client({ connectionString: env.HYPERDRIVE.connectionString });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

// Compatibility helper: some files import getClient()
// Keep this wrapper so index.ts can use a consistent name.
export async function getClient(env: any) {
  // If your db.ts already creates the client and connects it, reuse that logic.
  // Typical pattern:
  // const client = new Client({ connectionString: env.HYPERDRIVE.connectionString });
  // await client.connect();
  // return client;

  // If you already have an existing exported function that returns a connected client,
  // call it here instead. For example, if you have `export async function db(env) { ... }`,
  // then:
  // return db(env);

  // ---- Default implementation (safe) ----
  const { Client } = await import("pg");
  const client = new Client({ connectionString: env.HYPERDRIVE.connectionString });
  await client.connect();
  return client;
}