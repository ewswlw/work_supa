# AI Dev Tasks for Cursor - Step-by-Step Guide

Based on the GitHub repository, here's a comprehensive guide to using the AI Dev Tasks workflow in Cursor:

## 🎯 Overview
This workflow helps you build complex features with AI assistance by breaking down the development process into manageable, reviewable steps.

## 📋 Prerequisites
1. **Cursor Editor** with AI Agent access
2. **Download the .mdc files** from the repository:
   - `create-prd.mdc`
   - `generate-tasks.mdc` 
   - `process-task-list.mdc`
3. **Cursor Pro Plan** (recommended for better AI model performance)

## 🚀 Step-by-Step Workflow

### Step 1: Create a Product Requirement Document (PRD)

**Purpose**: Define what you're building, for whom, and why.

**Instructions**:
1. Open Cursor's Agent chat
2. Use the following command:
   ```
   Use @create-prd.mdc
   Here's the feature I want to build: [Describe your feature in detail]
   Reference these files to help you: [Optional: @file1.py @file2.ts]
   ```
3. **Pro Tip**: Use MAX mode in Cursor for more comprehensive PRD generation

**Expected Output**: A detailed PRD file (e.g., `MyFeature-PRD.md`)

---

### Step 2: Generate Task List from PRD

**Purpose**: Break down the PRD into granular, actionable tasks.

**Instructions**:
1. In Cursor's Agent chat, use your generated PRD:
   ```
   Now take @MyFeature-PRD.md and create tasks using @generate-tasks.mdc
   ```
2. Replace `@MyFeature-PRD.md` with your actual PRD filename

**Expected Output**: A structured task list with tasks and sub-tasks

---

### Step 3: Review Your Task List

**Purpose**: Examine the generated roadmap before implementation.

**Instructions**:
1. Review the task list structure
2. Ensure tasks are logical and complete
3. Make any necessary adjustments to the task order or descriptions

---

### Step 4: Start Task Implementation

**Purpose**: Begin methodical, step-by-step implementation with AI assistance.

**Instructions**:
1. In Cursor's Agent chat, start with the first task:
   ```
   Please start on task 1.1 and use @process-task-list.mdc
   ```
2. **Important**: Only reference `@process-task-list.mdc` for the first task - it guides subsequent tasks automatically

**What Happens**: The AI will:
- Work on the specific task
- Show you the changes made
- Wait for your approval before proceeding

---

### Step 5: Review, Approve, and Progress

**Purpose**: Maintain quality control and ensure correct implementation.

**Instructions**:
1. **Review each completed task** carefully
2. **If changes are good**: Reply with "yes" or similar affirmative
3. **If changes need adjustment**: Provide specific feedback for corrections
4. **Repeat** for each task until feature completion

**Progress Tracking**: You'll see tasks marked as complete, giving visual progress feedback

---

## 💡 Success Tips

### Essential Recommendations
- **Be Specific**: Provide detailed feature descriptions and clear instructions
- **Use Cursor Pro**: Free version models may struggle with structured instructions
- **MAX Mode for PRDs**: Use MAX mode for higher-quality PRD generation
- **Accurate File Tagging**: Always use correct filenames when referencing (e.g., `@MyFeature-PRD.md`)
- **Patience & Iteration**: Be prepared to guide and correct the AI as needed

### Troubleshooting
- **AI gets confused**: Break down tasks into smaller sub-tasks
- **Code quality issues**: Provide more specific requirements in PRD
- **Task execution problems**: Review and clarify task descriptions

---

## 🗂️ File Structure
After completing the workflow, you'll have:
```
project/
├── MyFeature-PRD.md          # Product Requirements Document
├── MyFeature-TaskList.md     # Generated task breakdown
├── [implementation files]     # AI-generated code files
└── .mdc files/               # Workflow command files
    ├── create-prd.mdc
    ├── generate-tasks.mdc
    └── process-task-list.mdc
```

## 🎯 Expected Benefits
- **Structured Development**: Clear process from idea to code
- **Quality Control**: Step-by-step verification of AI output
- **Complexity Management**: Large features broken into manageable pieces
- **Improved Reliability**: More dependable than single large AI prompts
- **Clear Progress Tracking**: Visual representation of completed work

This workflow transforms chaotic AI development into a systematic, reviewable process that maintains quality while leveraging AI capabilities effectively.
