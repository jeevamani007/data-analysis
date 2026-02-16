"""
Sankey Diagram Generator - Optimized for All Domains
Generates Sankey diagram data from case activities across all domains.
No hardcoding - derives everything from user case activities and events.
Works with: Banking, Healthcare, Insurance, Finance, Retail
"""

from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict


def extract_event_sequence(case_item: Dict[str, Any]) -> List[str]:
    """
    Extract event sequence from case details.
    REUSES the existing case ID sequence logic from domain analyzers.
    Works with all domains: Banking, Healthcare, Insurance, Finance, Retail
    event_sequence is already sorted by timestamp/order by the analyzers.
    No hardcoding - works with any domain structure.
    
    Args:
        case_item: Case detail dictionary
    
    Returns:
        List of event names (already in correct order from case ID logic)
    """
    if not case_item:
        return []
    
    # Primary: use event_sequence if available (already sorted by case ID logic)
    # This is the sequence that was already built by the domain analyzers
    # (banking_analyzer, healthcare_analyzer, retail_analyzer, etc.) respecting case ID rules:
    # - Sorted by timestamp (ascending)
    # - Case IDs assigned in ascending order
    # - Activities within each case sorted by timestamp
    event_sequence = case_item.get('event_sequence')
    if event_sequence:
        if isinstance(event_sequence, list):
            events = []
            for e in event_sequence:
                if e is None:
                    continue
                event_str = str(e).strip()
                # Filter out invalid event values
                if (event_str and 
                    event_str.lower() not in ('none', 'null', 'unknown', 'other', '')):
                    events.append(event_str)
            if events:
                # Return as-is - already in correct order from case ID logic
                return events
    
    # Fallback 1: extract from activities array (preserve order)
    # Activities are already sorted by timestamp in the analyzers
    activities = case_item.get('activities')
    if activities and isinstance(activities, list):
        events = []
        # Preserve the order from activities (already sorted by case ID logic)
        for activity in activities:
            if not activity:
                continue
            
            # Handle both dict and direct event value
            event_value = None
            if isinstance(activity, dict):
                # Check multiple possible fields for event name
                event_value = (activity.get('event') or 
                              activity.get('event_name') or 
                              activity.get('type') or 
                              activity.get('name') or 
                              activity.get('step'))
            elif isinstance(activity, str):
                event_value = activity
            
            if event_value:
                event_str = str(event_value).strip()
                # Filter out invalid event values
                if (event_str and 
                    event_str.lower() not in ('none', 'null', 'unknown', 'other', '')):
                    events.append(event_str)
        
        if events:
            return events
    
    # Fallback 2: try to find events in other possible fields
    # Some domains might store events differently
    possible_fields = ['events', 'event_list', 'sequence', 'steps']
    for field in possible_fields:
        field_value = case_item.get(field)
        if field_value and isinstance(field_value, list):
            events = [str(e).strip() for e in field_value if e and str(e).strip()]
            if events:
                return events
    
    return []


def calculate_event_positions(case_details: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Calculate event position weights based on actual sequence positions.
    REUSES the case ID sequence logic - events are already in correct order
    from the domain analyzers (sorted by timestamp, respecting case ID rules).
    No hardcoding - derives from user data.
    
    Args:
        case_details: List of case detail dictionaries
    
    Returns:
        Dictionary mapping event names to position statistics
    """
    event_positions = defaultdict(list)
    event_counts = defaultdict(int)
    
    # Use the event_sequence that's already built by case ID logic
    # This sequence respects the rules: sorted by timestamp, case ID ascending
    for case in case_details:
        event_sequence = extract_event_sequence(case)
        # event_sequence is already in correct order from case ID logic
        
        for position, event in enumerate(event_sequence):
            if not event:
                continue
            
            event_key = str(event).strip()
            if not event_key:
                continue
            
            # Store position as-is (already correct from case ID sequence)
            event_positions[event_key].append(position)
            event_counts[event_key] += 1
    
    # Calculate average position for each event
    # This preserves the sequence order from case ID logic
    avg_positions = {}
    for event, positions in event_positions.items():
        if positions:
            avg_positions[event] = {
                'average': sum(positions) / len(positions),
                'count': event_counts[event],
                'min': min(positions),
                'max': max(positions)
            }
    
    return avg_positions


def format_event_name(event_name: str) -> str:
    """
    Format event name for display (proper capitalization, spacing).
    No hardcoding - handles any event name from user data.
    
    Args:
        event_name: Raw event name from case data
    
    Returns:
        Formatted event name for display
    """
    if not event_name:
        return ''
    
    formatted = str(event_name).strip()
    if not formatted:
        return ''
    
    # Replace underscores and dashes with spaces
    formatted = formatted.replace('_', ' ').replace('-', ' ')
    
    # Title case: capitalize first letter of each word
    words = formatted.split()
    formatted_words = []
    
    for word in words:
        if not word:
            continue
        word_trimmed = word.strip()
        if not word_trimmed:
            continue
        
        # Handle acronyms (keep uppercase if all caps and short)
        if word_trimmed.isupper() and len(word_trimmed) <= 5 and word_trimmed.isalnum():
            formatted_words.append(word_trimmed)
        else:
            # Capitalize first letter, lowercase rest
            formatted_words.append(word_trimmed.capitalize())
    
    formatted = ' '.join(formatted_words)
    
    # Handle common acronyms (case-insensitive replacement)
    acronym_replacements = {
        'Kyc': 'KYC',
        'Upi': 'UPI',
        'Id': 'ID',
        'Api': 'API',
        'Dob': 'DOB',
        'Emi': 'EMI',
        'Cr': 'CR',
        'Dr': 'DR'
    }
    
    for old, new in acronym_replacements.items():
        # Case-insensitive replacement
        import re
        formatted = re.sub(r'\b' + re.escape(old) + r'\b', new, formatted, flags=re.IGNORECASE)
    
    return formatted.strip()


def assign_events_to_stages(event_positions: Dict[str, Dict[str, Any]], all_events: set) -> Dict[str, int]:
    """
    Assign events to sequential stages/columns based on their average position.
    Groups events that occur at similar positions into the same stage.
    Creates a linear left-to-right flow structure.
    
    Args:
        event_positions: Dictionary mapping event names to position statistics
        all_events: Set of all unique event names
    
    Returns:
        Dictionary mapping event names to stage numbers (0, 1, 2, ...)
    """
    if not all_events:
        return {}
    
    # Get average positions for all events
    event_avg_positions = {}
    for event in all_events:
        pos_info = event_positions.get(event, {})
        avg_pos = pos_info.get('average', 999)
        event_avg_positions[event] = avg_pos
    
    if not event_avg_positions:
        return {event: 0 for event in all_events}
    
    # Find min and max positions to determine stage range
    min_pos = min(event_avg_positions.values())
    max_pos = max(event_avg_positions.values())
    pos_range = max_pos - min_pos
    
    if pos_range == 0:
        # All events at same position - put them all in stage 0
        return {event: 0 for event in all_events}
    
    # Determine number of stages based on position range
    # Use dynamic staging: group events that are close together
    # More stages = more granular, fewer stages = more grouped
    num_stages = max(3, min(10, int(pos_range) + 1))
    
    # Assign each event to a stage based on normalized position
    event_stages = {}
    for event, avg_pos in event_avg_positions.items():
        # Normalize position to [0, 1] range
        normalized_pos = (avg_pos - min_pos) / pos_range if pos_range > 0 else 0
        # Assign to stage (0 to num_stages-1)
        stage = min(int(normalized_pos * num_stages), num_stages - 1)
        event_stages[event] = stage
    
    return event_stages


def generate_sankey_data(case_details: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate Sankey diagram data from case details.
    LINEAR PIPELINE STRUCTURE: Start Process → Events → End Process
    Structure: [Start Process (big box)] → [Event 1] → [Event 2] → ... → [End Process (big box)]
    All flows are strictly forward (left to right), creating a linear pipeline.
    No hardcoding - derives everything from user case activities.
    Works with all domains: Banking, Healthcare, Insurance, Finance, Retail
    
    Args:
        case_details: List of case detail dictionaries with 'activities' and/or 'event_sequence'
    
    Returns:
        Dictionary with nodes, links, and metadata for Sankey visualization
    """
    if not case_details:
        return {
            'success': False,
            'error': 'No case details provided'
        }
    
    # Calculate event positions to order them properly
    event_positions = calculate_event_positions(case_details)
    
    # Collect all unique events (nodes) from user data (no hardcoding)
    # Works with all domains: Banking, Healthcare, Insurance, Finance, Retail
    all_events = set()
    total_events_found = 0
    first_events = set()  # Events that appear first in sequences
    last_events = set()   # Events that appear last in sequences
    
    for case in case_details:
        event_sequence = extract_event_sequence(case)
        total_events_found += len(event_sequence)
        
        for event in event_sequence:
            event_key = str(event).strip()
            if event_key:
                all_events.add(event_key)
        
        # Track first and last events for Start/End connections
        if event_sequence:
            if event_sequence[0]:
                first_events.add(str(event_sequence[0]).strip())
            if len(event_sequence) > 1 and event_sequence[-1]:
                last_events.add(str(event_sequence[-1]).strip())
    
    if not all_events:
        # Provide helpful error message with detailed debugging info
        sample_case = case_details[0] if case_details else {}
        available_keys = ', '.join(sample_case.keys()) if sample_case else 'none'
        
        # Try to extract from sample case to show what we're looking for
        sample_event_sequence = extract_event_sequence(sample_case)
        
        debug_info = (
            f"Total cases: {len(case_details)}, Total events found: {total_events_found}\n"
            f"Sample case keys: {available_keys}\n"
            f"Sample event_sequence: {sample_case.get('event_sequence', 'not found')}\n"
            f"Sample activities: {len(sample_case.get('activities', [])) if sample_case.get('activities') else 'not found'} items\n"
            f"Extracted sequence: {sample_event_sequence[:3] if sample_event_sequence else 'empty'}"
        )
        
        return {
            'success': False,
            'error': (
                f'No events found in case details.\n\n'
                f'Available fields: {available_keys}\n'
                f'Expected fields: event_sequence or activities\n'
                f'Debug: {debug_info}'
            )
        }
    
    # Assign events to sequential stages (columns) for linear flow
    event_stages = assign_events_to_stages(event_positions, all_events)
    
    # Sort events by stage first, then by average position within stage, then by frequency
    # This creates a linear left-to-right flow: Stage 0 events → Stage 1 events → Stage 2 events → ...
    event_list = sorted(
        all_events,
        key=lambda e: (
            event_stages.get(e, 999),  # Primary: stage (ensures linear flow)
            event_positions.get(e, {}).get('average', 999),  # Secondary: position within stage
            -event_positions.get(e, {}).get('count', 0),     # Tertiary: frequency (desc)
            e  # Quaternary: alphabetical
        )
    )
    
    # Add Start Process and End Process nodes
    START_NODE_NAME = 'Start Process'
    END_NODE_NAME = 'End Process'
    
    # Create node list: [Start Process] + [Events] + [End Process]
    all_nodes_list = [START_NODE_NAME] + event_list + [END_NODE_NAME]
    
    # Create node mapping: event_name -> node_index
    node_map = {event: idx + 1 for idx, event in enumerate(event_list)}  # +1 because Start is at index 0
    node_map[START_NODE_NAME] = 0
    node_map[END_NODE_NAME] = len(all_nodes_list) - 1
    
    # Count transitions (from -> to) across all cases
    # CRITICAL: Only allow forward transitions (earlier stage → later stage)
    # This ensures linear flow and prevents backward/looping connections
    transition_counts = defaultdict(int)
    case_transitions = defaultdict(list)
    transition_details = defaultdict(lambda: {'cases': []})
    start_to_first = defaultdict(int)  # Start → First events
    last_to_end = defaultdict(int)      # Last events → End
    
    for case in case_details:
        case_id = case.get('case_id')
        # Handle different user ID field names across domains (no hardcoding)
        user_id = (
            case.get('user_id') or 
            case.get('customer_id') or 
            case.get('patient_id') or 
            'unknown'
        )
        event_sequence = extract_event_sequence(case)
        
        if not event_sequence:
            continue
        
        # Connect Start → First event
        if event_sequence:
            first_event = str(event_sequence[0]).strip()
            if first_event:
                start_to_first[first_event] += 1
                key = (START_NODE_NAME, first_event)
                if case_id not in case_transitions[key]:
                    case_transitions[key].append(case_id)
                    transition_details[key]['cases'].append({
                        'case_id': case_id,
                        'user_id': user_id
                    })
        
        # Connect Last event → End
        if len(event_sequence) > 1:
            last_event = str(event_sequence[-1]).strip()
            if last_event:
                last_to_end[last_event] += 1
                key = (last_event, END_NODE_NAME)
                if case_id not in case_transitions[key]:
                    case_transitions[key].append(case_id)
                    transition_details[key]['cases'].append({
                        'case_id': case_id,
                        'user_id': user_id
                    })
        
        # Track transitions between events (Event → Event)
        if len(event_sequence) >= 2:
            for i in range(len(event_sequence) - 1):
                from_event = str(event_sequence[i]).strip()
                to_event = str(event_sequence[i + 1]).strip()
                
                if from_event and to_event:
                    # Get stages for both events
                    from_stage = event_stages.get(from_event, 999)
                    to_stage = event_stages.get(to_event, 999)
                    
                    # ONLY allow forward transitions (from_stage < to_stage)
                    # This creates linear flow: earlier events → later events
                    if from_stage < to_stage:  # Strict forward only
                        key = (from_event, to_event)
                        transition_counts[key] += 1
                        
                        if case_id not in case_transitions[key]:
                            case_transitions[key].append(case_id)
                            transition_details[key]['cases'].append({
                                'case_id': case_id,
                                'user_id': user_id
                            })
                    elif from_stage == to_stage:
                        # Same stage: only allow if they're consecutive in the sequence
                        # This handles events that occur at similar times but in order
                        key = (from_event, to_event)
                        transition_counts[key] += 1
                        
                        if case_id not in case_transitions[key]:
                            case_transitions[key].append(case_id)
                            transition_details[key]['cases'].append({
                                'case_id': case_id,
                                'user_id': user_id
                            })
    
    # Build nodes array: Start Process + Events + End Process
    nodes = []
    
    # Start Process node (big box, stage -1)
    nodes.append({
        'id': 0,
        'name': START_NODE_NAME,
        'label': 'Start Process',
        'original_name': START_NODE_NAME,
        'position': -1,
        'frequency': len(case_details),
        'stage': -1,
        'is_start': True,
        'is_end': False
    })
    
    # Event nodes (middle)
    for idx, event in enumerate(event_list):
        pos_info = event_positions.get(event, {})
        stage = event_stages.get(event, 0)
        nodes.append({
            'id': idx + 1,
            'name': event,
            'label': format_event_name(event),
            'original_name': event,
            'position': pos_info.get('average', idx),
            'frequency': pos_info.get('count', 0),
            'stage': stage,
            'is_start': False,
            'is_end': False
        })
    
    # End Process node (big box, final stage)
    max_stage = max(event_stages.values()) if event_stages else 0
    nodes.append({
        'id': len(event_list) + 1,
        'name': END_NODE_NAME,
        'label': 'End Process',
        'original_name': END_NODE_NAME,
        'position': max_stage + 1,
        'frequency': len(case_details),
        'stage': max_stage + 1,
        'is_start': False,
        'is_end': True
    })
    
    # Build links array (flows between nodes)
    # Structure: Start → Events → End (strictly forward)
    links = []
    
    # Links: Start Process → First Events
    for first_event, count in start_to_first.items():
        if first_event in node_map:
            detail = transition_details.get((START_NODE_NAME, first_event), {'cases': []})
            links.append({
                'source': 0,  # Start Process
                'target': node_map[first_event],
                'value': count,
                'label': str(count),
                'from': START_NODE_NAME,
                'to': first_event,
                'from_formatted': 'Start Process',
                'to_formatted': format_event_name(first_event),
                'from_stage': -1,
                'to_stage': event_stages.get(first_event, 0),
                'case_ids': sorted(set(case_transitions.get((START_NODE_NAME, first_event), []))),
                'cases': detail.get('cases', [])
            })
    
    # Links: Event → Event (forward transitions only)
    for (from_event, to_event), count in transition_counts.items():
        if from_event in node_map and to_event in node_map:
            from_stage = event_stages.get(from_event, 999)
            to_stage = event_stages.get(to_event, 999)
            
            # Only forward transitions
            if from_stage <= to_stage:
                detail = transition_details[(from_event, to_event)]
                links.append({
                    'source': node_map[from_event],
                    'target': node_map[to_event],
                    'value': count,
                    'label': str(count),
                    'from': from_event,
                    'to': to_event,
                    'from_formatted': format_event_name(from_event),
                    'to_formatted': format_event_name(to_event),
                    'from_stage': from_stage,
                    'to_stage': to_stage,
                    'case_ids': sorted(set(case_transitions[(from_event, to_event)])),
                    'cases': detail.get('cases', [])
                })
    
    # Links: Last Events → End Process
    for last_event, count in last_to_end.items():
        if last_event in node_map:
            detail = transition_details.get((last_event, END_NODE_NAME), {'cases': []})
            links.append({
                'source': node_map[last_event],
                'target': len(event_list) + 1,  # End Process
                'value': count,
                'label': str(count),
                'from': last_event,
                'to': END_NODE_NAME,
                'from_formatted': format_event_name(last_event),
                'to_formatted': 'End Process',
                'from_stage': event_stages.get(last_event, max_stage),
                'to_stage': max_stage + 1,
                'case_ids': sorted(set(case_transitions.get((last_event, END_NODE_NAME), []))),
                'cases': detail.get('cases', [])
            })
    
    # Calculate statistics
    total_cases = len(case_details)
    total_transitions = sum(transition_counts.values()) + sum(start_to_first.values()) + sum(last_to_end.values())
    unique_paths = len(transition_counts) + len(start_to_first) + len(last_to_end)
    unique_users = len(set(
        c.get('user_id') or c.get('customer_id') or c.get('patient_id') or 'unknown'
        for c in case_details
    ))
    
    # Calculate stage statistics
    num_stages = max_stage + 3 if event_stages else 3  # +3 for Start, events, End
    events_per_stage = {}
    events_per_stage[-1] = 1  # Start Process
    events_per_stage[max_stage + 1] = 1  # End Process
    for event, stage in event_stages.items():
        events_per_stage[stage] = events_per_stage.get(stage, 0) + 1
    
    return {
        'success': True,
        'nodes': nodes,
        'links': links,
        'metadata': {
            'total_cases': total_cases,
            'total_transitions': total_transitions,
            'unique_paths': unique_paths,
            'unique_events': len(event_list),
            'unique_users': unique_users,
            'num_stages': num_stages,
            'events_per_stage': events_per_stage,
            'linear_flow': True,  # Indicates linear structure
            'has_start_end': True  # Indicates Start/End nodes
        }
    }


def generate_sankey_data_by_user(case_details: List[Dict[str, Any]], user_id: str) -> Dict[str, Any]:
    """
    Generate Sankey diagram data filtered by a specific user.
    No hardcoding - works with any user ID field name.
    
    Args:
        case_details: List of case detail dictionaries
        user_id: User ID to filter cases
    
    Returns:
        Sankey diagram data for the specified user
    """
    user_cases = [
        case for case in case_details 
        if (
            case.get('user_id') == user_id or 
            case.get('customer_id') == user_id or 
            case.get('patient_id') == user_id
        )
    ]
    return generate_sankey_data(user_cases)


def generate_sankey_data_by_case(case_details: List[Dict[str, Any]], case_id: int) -> Dict[str, Any]:
    """
    Generate Sankey diagram data for a single case.
    
    Args:
        case_details: List of case detail dictionaries
        case_id: Case ID to filter
    
    Returns:
        Sankey diagram data for the specified case
    """
    case = next((c for c in case_details if c.get('case_id') == case_id), None)
    if not case:
        return {
            'success': False,
            'error': f'Case ID {case_id} not found'
        }
    return generate_sankey_data([case])
