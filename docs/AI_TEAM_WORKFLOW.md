# AI Team Workflow Documentation

This document explains how the sequential AI team workflow operates in the Airflow Failure Analysis System.

## 🤖 Team Overview

The system uses a specialized team of 4 AI agents that work sequentially, each with specific input/output responsibilities:

1. **LogIngestor** → 2. **RootCauseAnalyst** → 3. **FixPlanner** → 4. **Verifier**

## 📋 Agent Details

### 1. LogIngestor
**Purpose**: Extract and summarize error information from raw Airflow logs

**Input**:
- Raw Airflow log text (error focus + log tail)

**Process**:
1. Filter out non-error entries (info, debug, success messages)
2. Extract error messages, stack traces, failing operators
3. Identify retry attempts and timing information
4. Focus on the most critical error messages and their context

**Output**:
- Clean error summary (maximum 10 lines) containing only error-related information

**Knowledge Base Usage**:
- No knowledge base search needed (pure log processing)

### 2. RootCauseAnalyst
**Purpose**: Analyze error summary and identify root cause using knowledge base

**Input**:
- Error summary from LogIngestor

**Process**:
1. Search the knowledge base for similar error patterns and solutions
2. Analyze the error summary to understand what went wrong
3. Identify the PRIMARY root cause (not symptoms) using knowledge base insights
4. Determine the failure category: network, dependency, config, code, infra, security, design, transient, other
5. Explain why the error occurred based on documentation and past cases
6. Assess confidence level (0.0-1.0)

**Output**:
- Root cause analysis with category and confidence

**Knowledge Base Usage**:
- Searches for similar error patterns
- References documented solutions and past cases
- Uses knowledge base insights for root cause identification

### 3. FixPlanner
**Purpose**: Create solutions based on root cause analysis using knowledge base

**Input**:
- Root cause analysis from RootCauseAnalyst

**Process**:
1. Search the knowledge base for documented solutions to similar problems
2. Take the root cause analysis and understand the problem
3. Find proven solutions from PDF documentation and past cases
4. Create 3-7 specific, actionable fix steps based on documented solutions
5. Include 2-6 prevention measures from best practices in knowledge base
6. Determine if the task needs a rerun after fixes

**Output**:
- Detailed fix plan with steps, prevention measures, and rerun recommendation

**Knowledge Base Usage**:
- **CRITICAL**: Always searches knowledge base first for documented solutions
- References specific solutions from PDF documents when available
- Creates fix steps based on proven, documented solutions

### 4. Verifier
**Purpose**: Consolidate all inputs into final JSON response with knowledge base validation

**Input**:
- Error summary from LogIngestor
- Root cause analysis from RootCauseAnalyst
- Fix plan from FixPlanner

**Process**:
1. Take the error summary from LogIngestor
2. Take the root cause analysis from RootCauseAnalyst
3. Take the fix plan from FixPlanner
4. Search knowledge base to validate solutions against documented best practices
5. Verify logical consistency between all inputs
6. Ensure solutions are based on proven methods from documentation
7. Consolidate everything into the required JSON format

**Output**:
- Final JSON response in the required schema

**Knowledge Base Usage**:
- Validates solutions against documented best practices
- Ensures solutions are based on proven methods from documentation

## 🔄 Workflow Sequence

```
Raw Airflow Logs
       ↓
   LogIngestor
       ↓
Error Summary
       ↓
RootCauseAnalyst ←→ Knowledge Base Search
       ↓
Root Cause Analysis
       ↓
FixPlanner ←→ Knowledge Base Search
       ↓
Fix Plan
       ↓
Verifier ←→ Knowledge Base Validation
       ↓
Final JSON Response
```

## 📊 Output Schema

The Verifier produces a JSON response with this exact schema:

```json
{
  "root_cause": "string",
  "category": "network" | "dependency" | "config" | "code" | "infra" | "security" | "design" | "transient" | "other",
  "fix_steps": ["string", "string", "string"],
  "prevention": ["string", "string"],
  "needs_rerun": true | false,
  "confidence": 0.0-1.0,
  "error_summary": "string"
}
```

## 🎯 Key Benefits

### Focused Processing
- Each agent has a specific role and receives only the input it needs
- No context overload - agents work with focused, relevant information
- Clear input/output boundaries between agents

### Knowledge Base Integration
- All agents (except LogIngestor) actively use the knowledge base
- Solutions are based on documented, proven methods
- Consistent validation against best practices

### Sequential Flow
- Natural progression from log analysis to solution
- Each agent builds on the previous agent's output
- Clear handoff points between agents

### Quality Assurance
- Verifier ensures all solutions are validated against knowledge base
- Logical consistency checks across all inputs
- Final output follows strict JSON schema

## 🔧 Configuration

The AI team is automatically configured when you set up the PDF knowledge base. All agents inherit the knowledge base configuration and will use it for:

- Searching for similar problems
- Finding documented solutions
- Validating proposed fixes
- Referencing best practices

See [Configuration Guide](CONFIGURATION.md) for detailed setup instructions.

## 🧪 Testing

Test the AI team workflow using the example DAG:

```python
# The example DAG includes various failure scenarios
# Each failure will trigger the sequential AI team analysis
# Check the output to see how each agent processes the information
```

## 📝 Best Practices

1. **PDF Knowledge Base**: Ensure your Problem_Solutions.pdf contains relevant, well-documented solutions
2. **Error Patterns**: Include common error patterns and their solutions in your PDF
3. **Solution Quality**: Document specific, actionable solutions rather than generic advice
4. **Regular Updates**: Keep your PDF knowledge base updated with new solutions and patterns
5. **Testing**: Regularly test with different failure scenarios to ensure the team works effectively

## 🔍 Troubleshooting

### Common Issues

1. **Generic Solutions**: If agents provide generic solutions, check that your PDF contains specific, documented solutions
2. **Low Confidence**: If confidence scores are low, ensure your knowledge base has relevant examples
3. **Missing Solutions**: If agents can't find solutions, verify your PDF is properly loaded and searchable
4. **JSON Parsing Errors**: If the final JSON is malformed, check that the Verifier has clear instructions

### Debug Tips

- Enable debug logging to see how each agent processes information
- Check the knowledge base search results in the logs
- Verify that your PDF contains the types of problems you're testing
- Monitor the sequential flow to ensure proper handoffs between agents
