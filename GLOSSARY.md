# Airflow Failure Responder

Turns a failed Airflow task into an explained alert in Microsoft Teams: what broke, why, and how past incidents were fixed.

## Language

### The failure

**Failure**:
A task attempt that ended in the failed state, identified by DAG, run, task and try number.
_Avoid_: Error, crash, incident (for the current event)

**Task log**:
The log of the failed attempt, cropped to the window around the error and redacted before anything else sees it.
_Avoid_: Raw log, full log

### Knowledge

**Incident record**:
One row of the team's Confluence incident table: a past problem, where it happened, and how it was resolved.
_Avoid_: Doc, document, knowledge row, evidence, runbook entry

**Knowledge base**:
The searchable collection of incident records.
_Avoid_: KB documents, PDF knowledge

**Matched incident**:
An incident record that the analysis says applies to the current failure.
_Avoid_: Used doc, source

### The outcome

**Analysis**:
The explanation of a failure: summary, root cause, category, fix steps, prevention and confidence.
_Avoid_: Report, diagnosis, result

**Root cause**:
The condition that started the failure, as opposed to the symptoms in the task log.
_Avoid_: Reason, error message

**Alert card**:
The Teams message that carries an analysis for one failure.
_Avoid_: Notification, message, card

**Fingerprint**:
Exception type plus the error message with volatile parts (ids, numbers, timestamps, IPs) removed. Failures with the same fingerprint share a root cause, whatever DAG they are in.
_Avoid_: Hash, error key, signature

**Dedup summary**:
The alert card sent after a cooldown, saying how many more failures had the same fingerprint and which tasks they were.
_Avoid_: Digest, rollup, N-tasks card

**Feedback**:
A person's verdict on an analysis (`Doğru`, `Kısmen`, `Yanlış`, or `unanswered` after 24 hours), with an optional note. It is reused for later failures with the same fingerprint.
_Avoid_: Rating, review, approval

**Fallback alert**:
An alert card sent without an analysis, carrying only the last error line, so a failure is never silent.
_Avoid_: Error card, empty alert
