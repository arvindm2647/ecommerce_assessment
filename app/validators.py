from datetime import datetime

def validate_event(event):
    errors = []
    
    required_fields = ['user_id', 'event_type', 'event_timestamp']
    for field in required_fields:
        if field not in event:
            errors.append(f"Missing required field: {field}")
    
    if 'event_type' in event:
        valid_types = ['page_view', 'add_to_cart', 'purchase']
        if event['event_type'] not in valid_types:
            errors.append(f"Invalid event_type. Must be one of {valid_types}")
    
    if 'event_timestamp' in event:
        try:
            datetime.fromisoformat(event['event_timestamp'].replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            errors.append("Invalid timestamp format. Must be ISO8601")
    
    if 'user_id' in event:
        try:
            int(event['user_id'])
        except (ValueError, TypeError):
            errors.append("user_id must be an integer")
    
    if 'product_id' in event and event['product_id'] is not None:
        try:
            int(event['product_id'])
        except (ValueError, TypeError):
            errors.append("product_id must be an integer")
    
    return errors

def validate_date_format(date_string):
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False