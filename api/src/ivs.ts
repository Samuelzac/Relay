import { AwsClient } from "aws4fetch";

export type IvsCreateResult = {
  channelArn: string;
  ingestEndpoint: string;
  playbackUrl: string;
  streamKey: string;
};

export async function createIvsChannel(env: any, eventId: string): Promise<IvsCreateResult> {
  const aws = new AwsClient({
    accessKeyId: env.AWS_ACCESS_KEY_ID,
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    region: env.AWS_REGION,
    service: "ivs"
  });

  const res = await aws.fetch(`${env.IVS_API_ENDPOINT}/CreateChannel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name: `event-${eventId}`,
      latencyMode: "LOW",
      type: "STANDARD"
    })
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`IVS CreateChannel failed: ${res.status} ${text}`);
  }

  const data = await res.json<any>();
  return {
    channelArn: data.channel?.arn,
    ingestEndpoint: data.channel?.ingestEndpoint,
    playbackUrl: data.channel?.playbackUrl,
    streamKey: data.streamKey?.value
  };
}

export async function deleteIvsChannel(env: any, channelArn: string) {
  const aws = new AwsClient({
    accessKeyId: env.AWS_ACCESS_KEY_ID,
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    region: env.AWS_REGION,
    service: "ivs"
  });

  const res = await aws.fetch(`${env.IVS_API_ENDPOINT}/DeleteChannel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ arn: channelArn })
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`IVS DeleteChannel failed: ${res.status} ${text}`);
  }
}
