from flask import Blueprint, jsonify, request
from app.database import get_db_cursor
from app.validators import validate_event, validate_date_format
from datetime import datetime

api_bp = Blueprint('api', __name__)

@api_bp.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    }), 200

@api_bp.route('/orders', methods=['GET'])
def get_user_orders():
    user_id = request.args.get('user_id')
    
    if not user_id:
        return jsonify({'error': 'user_id parameter is required'}), 400
    
    try:
        user_id = int(user_id)
    except ValueError:
        return jsonify({'error': 'user_id must be an integer'}), 400
    
    query = """
        SELECT 
            o.order_id,
            o.order_date,
            o.status,
            o.total_amount,
            json_agg(
                json_build_object(
                    'product_id', p.product_id,
                    'product_name', p.product_name,
                    'quantity', oi.quantity,
                    'unit_price', oi.unit_price,
                    'subtotal', oi.quantity * oi.unit_price
                )
            ) as items
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        LEFT JOIN products p ON oi.product_id = p.product_id
        WHERE o.user_id = %s
        GROUP BY o.order_id, o.order_date, o.status, o.total_amount
        ORDER BY o.order_date DESC
        LIMIT 10
    """
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute(query, (user_id,))
            orders = cursor.fetchall()
            
            if not orders:
                return jsonify({'message': 'No orders found for this user', 'orders': []}), 200
            
            return jsonify({'orders': orders}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/metrics/daily-revenue', methods=['GET'])
def get_daily_revenue():
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    
    if not from_date or not to_date:
        return jsonify({'error': 'from and to parameters are required (format: YYYY-MM-DD)'}), 400
    
    if not validate_date_format(from_date) or not validate_date_format(to_date):
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    
    query = """
        SELECT 
            DATE(order_date) AS order_day,
            SUM(total_amount) AS daily_revenue,
            COUNT(order_id) AS total_orders
        FROM orders
        WHERE status = 'paid'
            AND DATE(order_date) BETWEEN %s AND %s
        GROUP BY DATE(order_date)
        ORDER BY order_day
    """
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute(query, (from_date, to_date))
            results = cursor.fetchall()
            
            return jsonify({'daily_revenue': results}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/ingest/events', methods=['POST'])
def ingest_events():
    events = request.json
    
    if not isinstance(events, list):
        return jsonify({'error': 'Request body must be a JSON array of events'}), 400
    
    if not events:
        return jsonify({'error': 'Events array cannot be empty'}), 400
    
    valid_events = []
    invalid_events = []
    
    for idx, event in enumerate(events):
        errors = validate_event(event)
        if errors:
            invalid_events.append({
                'index': idx,
                'event': event,
                'errors': errors
            })
        else:
            valid_events.append(event)
    
    inserted_count = 0
    if valid_events:
        insert_query = """
            INSERT INTO events (user_id, event_type, product_id, event_timestamp)
            VALUES (%s, %s, %s, %s)
        """
        
        try:
            with get_db_cursor(commit=True) as cursor:
                for event in valid_events:
                    cursor.execute(insert_query, (
                        event['user_id'],
                        event['event_type'],
                        event.get('product_id'),
                        event['event_timestamp']
                    ))
                    inserted_count += 1
        except Exception as e:
            return jsonify({'error': f'Database error: {str(e)}'}), 500
    
    response = {
        'inserted': inserted_count,
        'rejected': len(invalid_events),
        'invalid_events': invalid_events
    }
    
    if invalid_events:
        return jsonify(response), 207
    else:
        return jsonify(response), 201

@api_bp.route('/products/top', methods=['GET'])
def get_top_products():
    days = request.args.get('days', default=7, type=int)
    limit = request.args.get('limit', default=5, type=int)
    
    if days <= 0 or limit <= 0:
        return jsonify({'error': 'days and limit must be positive integers'}), 400
    
    query = """
        SELECT 
            p.product_id,
            p.product_name,
            p.category,
            SUM(oi.quantity) AS total_units_sold,
            SUM(oi.quantity * oi.unit_price) AS total_revenue
        FROM products p
        JOIN order_items oi ON p.product_id = oi.product_id
        JOIN orders o ON oi.order_id = o.order_id
        WHERE o.status = 'paid'
        GROUP BY p.product_id, p.product_name, p.category
        ORDER BY total_revenue DESC
        LIMIT %s
    """
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute(query, (limit,))
            products = cursor.fetchall()
            
            return jsonify({'top_products': products}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500