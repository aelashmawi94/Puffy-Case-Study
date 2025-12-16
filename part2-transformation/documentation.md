## Pipeline Architecture

The pipeline follows a layered architecture after validation, where each stage incrementally enriches the data before passing it downstream:

- **Event enrichment**
- **Sessionization**
- **Conversion identification**
- **Attribution modeling**

Each layer depends only on validated outputs from the previous step, ensuring that issues are detected early and do not silently propagate through the pipeline.

## Core Definitions and Design Decisions

### Users

A user is defined as an entity with a non-null `client_id`. Anonymous events are retained for session-level analysis but are not promoted to user-level entities.

**Trade-offs**
- Avoids inflating user counts through fingerprinting or inference  
- Limits user-level metrics to identified users only  

### Sessions

A session is defined as a sequence of events from the same user separated by no more than 30 minutes of inactivity. Anonymous events are sessionized independently to avoid collapsing all anonymous traffic into a single session.

**Trade-offs**
- Preserves realistic session counts  
- Avoids artificially long sessions  
- Does not infer anonymous identity across sessions  

### Conversions

Conversions are identified via the `checkout_completed` event and tied to a transaction identifier. Only identified users are eligible for conversions.

**Trade-offs**
- Ensures revenue can be reliably joined and attributed  
- Excludes anonymous conversions if present  

### Attribution

Attribution is modeled using a 7-day lookback window and includes:

- First-click attribution  
- Last-click attribution  
- Direct attribution when no eligible touchpoints exist  

An additional metric counts the number of distinct sessions leading to conversion within the attribution window.

**Trade-offs**
- Avoids overfitting with complex multi-touch models that the data does not justify  
