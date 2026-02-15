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


def generate_sankey_data(case_details: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate Sankey diagram data from case details.
    Optimized: orders events by sequence position, not alphabetically.
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
    
    for case in case_details:
        event_sequence = extract_event_sequence(case)
        total_events_found += len(event_sequence)
        
        for event in event_sequence:
            event_key = str(event).strip()
            if event_key:
                all_events.add(event_key)
    
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
    
    # Sort events by average position (REUSES case ID sequence logic)
    # Events are already ordered correctly in event_sequence by domain analyzers
    # This preserves that order in the Sankey diagram
    event_list = sorted(
        all_events,
        key=lambda e: (
            event_positions.get(e, {}).get('average', 999),  # Primary: position (preserves case ID sequence)
            -event_positions.get(e, {}).get('count', 0),     # Secondary: frequency (desc)
            e  # Tertiary: alphabetical
        )
    )
    
    # Create node mapping: event_name -> node_index
    node_map = {event: idx for idx, event in enumerate(event_list)}
    
    # Count transitions (from -> to) across all cases
    transition_counts = defaultdict(int)
    case_transitions = defaultdict(list)
    transition_details = defaultdict(lambda: {'cases': []})
    
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
        
        if len(event_sequence) < 2:
            continue
        
        # Track transitions in this case
        # REUSES the case ID sequence - transitions follow the exact order
        # from event_sequence which is already sorted by case ID logic
        for i in range(len(event_sequence) - 1):
            from_event = str(event_sequence[i]).strip()
            to_event = str(event_sequence[i + 1]).strip()
            
            if from_event and to_event:
                # This transition follows the case ID sequence order
                # (already sorted by timestamp, case ID ascending)
                key = (from_event, to_event)
                transition_counts[key] += 1
                
                if case_id not in case_transitions[key]:
                    case_transitions[key].append(case_id)
                    transition_details[key]['cases'].append({
                        'case_id': case_id,
                        'user_id': user_id
                    })
    
    # Build nodes array with formatted names
    nodes = []
    for idx, event in enumerate(event_list):
        pos_info = event_positions.get(event, {})
        nodes.append({
            'id': idx,
            'name': event,
            'label': format_event_name(event),
            'original_name': event,
            'position': pos_info.get('average', idx),
            'frequency': pos_info.get('count', 0)
        })
    
    # Build links array (flows between nodes)
    links = []
    for (from_event, to_event), count in transition_counts.items():
        if from_event in node_map and to_event in node_map:
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
                'case_ids': sorted(set(case_transitions[(from_event, to_event)])),
                'cases': detail.get('cases', [])
            })
    
    # Calculate statistics
    total_cases = len(case_details)
    total_transitions = sum(transition_counts.values())
    unique_paths = len(transition_counts)
    unique_users = len(set(
        c.get('user_id') or c.get('customer_id') or c.get('patient_id') or 'unknown'
        for c in case_details
    ))
    
    return {
        'success': True,
        'nodes': nodes,
        'links': links,
        'metadata': {
            'total_cases': total_cases,
            'total_transitions': total_transitions,
            'unique_paths': unique_paths,
            'unique_events': len(event_list),
            'unique_users': unique_users
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
