export type AgentMessage = {
  role: "system" | "user" | "assistant";
  content: string;
};

export type ToolCall = {
  tool: string;
  input: Record<string, unknown>;
};

export type ToolResult = {
  tool: string;
  output: Record<string, unknown>;
};

export interface ITool {
  readonly name: string;
  execute(input: Record<string, unknown>): Promise<Record<string, unknown>>;
}

export interface IAgentService {
  runWithTools(input: {
    messages: AgentMessage[];
    tools: ITool[];
  }): Promise<{
    outputText: string;
    toolCalls: ToolCall[];
    toolResults: ToolResult[];
  }>;
}
