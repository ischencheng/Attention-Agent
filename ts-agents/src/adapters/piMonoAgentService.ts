import type { AgentMessage, IAgentService, ITool, ToolCall, ToolResult } from "../core/interfaces.js";

export class PiMonoAgentService implements IAgentService {
  private readonly model: string;

  constructor(model: string) {
    this.model = model;
  }

  async runWithTools(input: { messages: AgentMessage[]; tools: ITool[] }): Promise<{
    outputText: string;
    toolCalls: ToolCall[];
    toolResults: ToolResult[];
  }> {
    // #region agent log
    fetch('http://127.0.0.1:7846/ingest/d084e36b-af1e-438e-bcf1-86032ad1b504',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'8490b6'},body:JSON.stringify({sessionId:'8490b6',runId:'pre-fix',hypothesisId:'H1',location:'piMonoAgentService.ts:15',message:'adapter entry',data:{configuredModel:this.model,toolsCount:input.tools.length,messagesCount:input.messages.length,hasGeminiKey:(process.env.GEMINI_API_KEY??'').length>0,hasGoogleKey:(process.env.GOOGLE_API_KEY??'').length>0},timestamp:Date.now()})}).catch(()=>{});
    // #endregion
    console.log("[adapter] entry", {
      configuredModel: this.model,
      hasGeminiKey: (process.env.GEMINI_API_KEY ?? "").length > 0,
      hasGoogleKey: (process.env.GOOGLE_API_KEY ?? "").length > 0,
    });
    const piAgentCore = await import("@mariozechner/pi-agent-core");
    const AgentCtor = (piAgentCore as Record<string, unknown>).Agent;
    if (typeof AgentCtor !== "function") {
      throw new Error("pi-mono adapter error: Agent constructor not found");
    }

    const agent = new (AgentCtor as new () => {
      setSystemPrompt: (p: string) => void;
      setModel?: (m: string) => void;
      setTools: (tools: Array<{ name: string; description: string; execute: (input: unknown) => Promise<unknown> }>) => void;
      prompt: (content: string) => Promise<void>;
      state: { messages: Array<{ role: string; content: unknown }> };
    })();
    // #region agent log
    fetch('http://127.0.0.1:7846/ingest/d084e36b-af1e-438e-bcf1-86032ad1b504',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'8490b6'},body:JSON.stringify({sessionId:'8490b6',runId:'pre-fix',hypothesisId:'H2',location:'piMonoAgentService.ts:31',message:'agent methods inspected',data:{hasSetModel:typeof agent.setModel==='function'},timestamp:Date.now()})}).catch(()=>{});
    // #endregion
    console.log("[adapter] methods", { hasSetModel: typeof agent.setModel === "function" });
    if (typeof agent.setModel === "function") {
      agent.setModel(this.model);
      // #region agent log
      fetch('http://127.0.0.1:7846/ingest/d084e36b-af1e-438e-bcf1-86032ad1b504',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'8490b6'},body:JSON.stringify({sessionId:'8490b6',runId:'pre-fix',hypothesisId:'H2',location:'piMonoAgentService.ts:37',message:'setModel applied',data:{configuredModel:this.model},timestamp:Date.now()})}).catch(()=>{});
      // #endregion
      console.log("[adapter] setModel applied", { configuredModel: this.model });
    }

    const systemMessages = input.messages.filter((m) => m.role === "system").map((m) => m.content);
    if (systemMessages.length > 0) {
      agent.setSystemPrompt(systemMessages.join("\n"));
    }

    const toolResults: ToolResult[] = [];
    agent.setTools(
      input.tools.map((tool) => ({
        name: tool.name,
        label: tool.name,
        description: `tool:${tool.name}`,
        parameters: {
          type: "object",
          additionalProperties: true,
          properties: {},
        },
        execute: async (rawInput: unknown) => {
          const inputObj =
            rawInput && typeof rawInput === "object" ? (rawInput as Record<string, unknown>) : {};
          const output = await tool.execute(inputObj);
          toolResults.push({ tool: tool.name, output });
          return {
            content: [{ type: "text", text: JSON.stringify(output) }],
            details: output,
          };
        },
      }))
    );

    const userPrompt = input.messages
      .filter((m) => m.role !== "system")
      .map((m) => `${m.role}: ${m.content}`)
      .join("\n");
    await agent.prompt(userPrompt);

    const lastAssistant = [...agent.state.messages].reverse().find((m) => m.role === "assistant");
    const outputText =
      typeof lastAssistant?.content === "string"
        ? lastAssistant.content
        : JSON.stringify(lastAssistant?.content ?? "");
    return {
      outputText,
      toolCalls: [],
      toolResults,
    };
  }
}
