Relay IVS Fix Pack

Copy these files into your repo and OVERWRITE the existing ones:

- src/index.ts
- src/awsIvs.ts
- src/awsIvsRealtime.ts
- src/awsSigV4.ts
- src/ivs.ts

Why this fixes playback
- Your Stage can be LIVE without HLS working.
- HLS requires an IVS Realtime *Composition* feeding an IVS *Channel*.
- This pack switches IVS Realtime calls to the correct AWS JSON-RPC endpoint:
  https://ivsrealtime.<region>.amazonaws.com/
  with x-amz-target IVSRealTimeService.CreateComposition etc.
- It also creates/stores encoderConfigurationArn + compositionArn in rtc_stage_endpoints.

Required env vars (Cloudflare Worker)
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_REGION (ap-northeast-1)

IAM permissions required (minimum)
- ivs:CreateChannel
- ivs:GetChannel
- ivs:DeleteChannel
- ivs-realtime:CreateStage
- ivs-realtime:DeleteStage
- ivs-realtime:CreateParticipantToken
- ivs-realtime:CreateEncoderConfiguration
- ivs-realtime:CreateComposition
- ivs-realtime:StopComposition
- ivs-realtime:GetComposition

Verify after deploy
1) Start an event with hls_enabled=true
2) Run:
   aws ivs-realtime list-compositions --region ap-northeast-1
You should see a composition ARN.
