/**
 * Script Bridge（本地定制，课题#67-487，不属于官方代码）
 *
 * 让外部脚本通过本机 HTTP + 固定 Header 鉴权直接调用 IZCodeTaskService，建对话/发消息/取消/
 * 轮询事件；taskId 与桌面端 UI 共用同一个 task 服务实例，脚本的操作会实时体现在桌面端窗口里。
 * 默认不启用：只有设置了 ZCODE_SCRIPT_BRIDGE_KEY 环境变量才会监听端口，不影响任何官方行为。
 */
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { IZCodeTaskService, type ServiceCollection } from "@zcode/services";

interface ScriptBridgeEvent {
  readonly seq: number;
  readonly time: number;
  readonly type: string;
  readonly taskId: string;
  readonly data?: unknown;
}

const SCRIPT_BRIDGE_EVENT_CAP = 1000;
const scriptBridgeEvents: ScriptBridgeEvent[] = [];
let scriptBridgeEventSeq = 0;

function pushScriptBridgeEvent(type: string, taskId: string, data?: unknown): void {
  scriptBridgeEventSeq += 1;
  scriptBridgeEvents.push({ seq: scriptBridgeEventSeq, time: Date.now(), type, taskId, data });
  if (scriptBridgeEvents.length > SCRIPT_BRIDGE_EVENT_CAP) scriptBridgeEvents.shift();
}

function writeJson(res: ServerResponse, status: number, value: unknown): void {
  const text = JSON.stringify(value);
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(text);
}

async function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks).toString("utf8");
}

const subscribedTaskIds = new Set<string>();
/** create 时记下 taskId→workspacePath，后续 prompt/cancel 只传 taskId 也能找到事件订阅目标。 */
const taskWorkspacePaths = new Map<string, string>();

/**
 * 每个 taskId 只订阅一次。
 *
 * 关键坑（课题#67-487 第三轮实测踩到）：`onDynamicStreamEvent(taskId)` 只读一个被动的全局
 * emitter，真正把 agent 的实时流事件接上这个 emitter 的订阅逻辑，只在 `onDynamicTaskEvent`
 * （带 workspacePath 的那个）内部才会建立——不调用它，流式事件永远是空的，只有 terminal/ready
 * 两头能收到。所以这里必须用 onDynamicTaskEvent，不能只用 onDynamicStreamEvent。
 */
function subscribeTaskEvents(taskService: IZCodeTaskService, taskId: string, workspacePath: string): void {
  if (subscribedTaskIds.has(taskId)) return;
  subscribedTaskIds.add(taskId);
  taskService.onDynamicTaskEvent({ workspacePath, taskId })((event) => {
    pushScriptBridgeEvent("stream", taskId, event);
    if (event.type === "task_token_usage_delta") {
      pushScriptBridgeEvent("usage", taskId, event.usage);
    }
  });
  taskService.onDynamicTaskTerminalOutcome(taskId)((outcome) => {
    pushScriptBridgeEvent("terminal", taskId, outcome);
  });
  taskService.onDynamicTaskReady(taskId)(() => {
    pushScriptBridgeEvent("ready", taskId);
  });
}

async function handleRequest(
  taskService: IZCodeTaskService,
  expectedKey: string,
  req: IncomingMessage,
  res: ServerResponse,
): Promise<void> {
  if (req.method !== "POST") {
    writeJson(res, 405, { error: "POST only" });
    return;
  }
  if (req.headers["x-script-bridge-key"] !== expectedKey) {
    writeJson(res, 401, { error: "unauthorized" });
    return;
  }
  let body: unknown;
  try {
    body = JSON.parse(await readBody(req));
  } catch {
    writeJson(res, 400, { error: "invalid JSON body" });
    return;
  }
  if (typeof body !== "object" || body === null || !("action" in body) || !("payload" in body)) {
    writeJson(res, 400, { error: 'expected {"action","payload"}' });
    return;
  }
  const { action, payload } = body as { action: unknown; payload: unknown };
  try {
    if (action === "create") {
      const params = payload as Parameters<IZCodeTaskService["createTask"]>[0];
      const result = await taskService.createTask(params);
      taskWorkspacePaths.set(result.taskId, result.workspacePath);
      subscribeTaskEvents(taskService, result.taskId, result.workspacePath);
      writeJson(res, 200, { ok: true, result });
      return;
    }
    if (action === "prompt") {
      const params = payload as Parameters<IZCodeTaskService["sendPrompt"]>[0] & {
        workspacePath?: string;
      };
      const workspacePath = params.workspacePath ?? taskWorkspacePaths.get(params.taskId);
      if (workspacePath !== undefined) subscribeTaskEvents(taskService, params.taskId, workspacePath);
      await taskService.sendPrompt(params);
      writeJson(res, 200, { ok: true });
      return;
    }
    if (action === "cancel") {
      const params = payload as Parameters<IZCodeTaskService["stopGeneration"]>[0];
      await taskService.stopGeneration(params);
      writeJson(res, 200, { ok: true });
      return;
    }
    if (action === "usage") {
      const params = payload as Parameters<IZCodeTaskService["getTaskTokenUsage"]>[0] & {
        workspacePath?: string;
      };
      const workspacePath = params.workspacePath ?? taskWorkspacePaths.get(params.taskId);
      if (workspacePath === undefined) {
        writeJson(res, 400, { error: "unknown taskId, pass workspacePath explicitly" });
        return;
      }
      const result = await taskService.getTaskTokenUsage({ ...params, workspacePath });
      writeJson(res, 200, { ok: true, result });
      return;
    }
    if (action === "events") {
      const sinceSeqValue = (payload as { sinceSeq?: unknown } | undefined)?.sinceSeq;
      const sinceSeq = typeof sinceSeqValue === "number" ? sinceSeqValue : 0;
      const events = scriptBridgeEvents.filter((event) => event.seq > sinceSeq);
      writeJson(res, 200, { ok: true, result: { events, lastSeq: scriptBridgeEventSeq } });
      return;
    }
    writeJson(res, 400, { error: `unknown action "${String(action)}"` });
  } catch (error) {
    writeJson(res, 500, { error: error instanceof Error ? error.message : String(error) });
  }
}

/** services 已经是桌面端 host 进程里和 UI 共用的同一个 ServiceCollection 实例。 */
export function startScriptBridge(services: ServiceCollection): void {
  const scriptBridgeKey = process.env.ZCODE_SCRIPT_BRIDGE_KEY;
  if (scriptBridgeKey === undefined || scriptBridgeKey.length === 0) return;
  const taskService = services.getOptional(IZCodeTaskService);
  if (!taskService) return;

  taskService.onError((error) => {
    pushScriptBridgeEvent("error", "", error);
  });

  const port = Number(process.env.ZCODE_SCRIPT_BRIDGE_PORT ?? "47901");
  const server = createServer((req, res) => {
    void handleRequest(taskService, scriptBridgeKey, req, res);
  });
  server.listen(port, "127.0.0.1");
}
