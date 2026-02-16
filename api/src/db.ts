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
