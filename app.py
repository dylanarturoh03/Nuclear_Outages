import logging
from math import ceil
from datetime import datetime, timedelta
from src.config import DB_DIR
from src.pipeline import run_ETLpipeline
import duckdb
from flask import Flask, request, jsonify, render_template

# Config logger
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log")
    ]
)

logger = logging.getLogger(__name__)
app = Flask(__name__)


def parse_date(x: str) -> datetime:
    formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%a, %d %b %Y %H:%M:%S %Z']
    for fmt in formats:
        try:
            return datetime.strptime(x, fmt)
        except ValueError:
            continue
    raise ValueError(f'Could not parse {x} into date.')


# Config / Constants
OPERATORS: dict[str, str] = {
    'eq': '=',
    'lt': '<',
    'gt': '>',
    'lte': '<=',
    'gte': '>='
}
# Params handled explicitly in get_data — excluded from filter building
RESERVED_PARAMS: set[str] = {
    'table',
    'order_by',
    'limit',
    'page',
    'sort_by'
}
TABLES: dict[str, dict[str, type]] = {
    'facilities': {
        'id': int,
        'name': str
    },
    'generators': {
        'id': int,
        'facility_id': int,
        'generator_number': int,
        'capacity': float
    },
    'outages': {
        'id': int,
        'period': parse_date,
        'generator_id': int,
        'outage': float
    }
}


# Helper functions
def get_last_date() -> str | None:
    try:
        # Raise exception so database doesn't get created early.
        if not DB_DIR.exists():
            raise duckdb.Error

        with duckdb.connect(database=DB_DIR) as con:
            query: str = 'SELECT MAX(period) FROM outages;'
            max_date: str = con.execute(query).fetchone()[0]
            if max_date is None:
                return None
            return (max_date + timedelta(days=1)).strftime('%Y-%m-%d')

    except duckdb.Error as e:
        logger.info(f'Failed to get last date: {e}.')
        raise duckdb.Error(
            'Failed to reach DB. Please check for the existance of DB.'
        )


def parse(arg: str) -> tuple[str, str]:
    if '_' in arg:
        args_k, args_ops = arg.rsplit('_', 1)
    else:
        return arg, OPERATORS['eq']

    if args_ops in OPERATORS:
        args_ops = OPERATORS[args_ops]
    else:
        args_k, args_ops = arg, OPERATORS['eq']
    return args_k, args_ops


def build_filters(table: str, args: dict) -> tuple[list, list]:
    filters = []
    params = []
    logger.debug('About to start building filters.')
    for k, v in args.items():
        if k not in RESERVED_PARAMS:
            args_k, args_ops = parse(k)

            if args_k not in TABLES[table]:
                raise ValueError(f'{args_k} is not a valid column.')

            col_type = TABLES[table][args_k]

            filters.append(f'{args_k} {args_ops} ?')

            try:
                params.append(col_type(v))
            except ValueError:
                raise ValueError(
                    f'{v} is an invalid value. '
                    f'Must be {col_type} '
                    f'but got {type(v)}'
                )

    return filters, params


# Routes
@app.route('/')
def index():
    logger.info('App running.')
    return render_template('index.html')


@app.route('/data')
def get_data():
    '''Prepare a Query to DuckDB Database.'''
    # Validate reserved_params from args
    table: str = request.args.get('table', 'outages')

    if table not in TABLES:
        return jsonify({'error': 'Invalid table request'}), 400

    sort_by: str = request.args.get('sort_by', 'id')

    if sort_by not in TABLES[table]:
        return jsonify({'error': 'Invalid sort column'}), 400

    order_by: str = request.args.get('order_by', 'ASC').upper()

    if order_by not in {'DESC', 'ASC'}:
        order_by = 'ASC'

    try:
        limit: int = int(request.args.get('limit', 10))
    except ValueError:
        return jsonify({
            'error': 'Invalid limit value, must be an integer'
            }), 400

    try:
        page: int = int(request.args.get('page', 1))
        offset: int = (page - 1) * limit
    except ValueError:
        return jsonify({
            'error': 'Invalid offset value, must be an integer'
            }), 400

    try:
        filters, params = build_filters(table, request.args)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    # Build query
    query = f'SELECT * FROM {table} '
    pag_query = f'SELECT COUNT(*) FROM {table}'
    if filters:
        q_filter = ' WHERE ' + ' AND '.join(filters)
        query += q_filter
        pag_query += q_filter
    query += f' ORDER BY {sort_by} {order_by} LIMIT ? OFFSET ?'

    # Execute query
    try:
        with duckdb.connect(database=DB_DIR) as con:
            # Query to get total number of rows
            n_rows = con.execute(pag_query, params).fetchone()[0]

            # Append limit and offset to main query.
            params.append(limit)
            params.append(offset)

            # Query to fetch main data.
            res = con.execute(query, params).fetchall()

            # Get column names.
            columns = [desc[0] for desc in con.description]
    except duckdb.Error as e:
        logger.error(f"DuckDB error: {e}")
        return jsonify({
            'error': 'Database error, please try again later.'
        }), 500

    data = [dict(zip(columns, row)) for row in res]

    total_pages: int = ceil(n_rows / limit)

    return jsonify({
        'data': data,
        'page': page,
        'limit': limit,
        'total': n_rows,
        'total_pages': total_pages
    }), 200


@app.route('/schema')
def get_schema():
    schema = {}

    for table, columns in TABLES.items():
        schema[table] = {}
        for col, typ in columns.items():
            if typ == parse_date:
                schema[table][col] = 'date'
            else:
                schema[table][col] = typ.__name__

    return jsonify(schema)


@app.route('/refresh')
def refresh():
    try:
        try:
            start_date: str | None = get_last_date()
        except Exception as e:
            logger.warning(str(e))
            logger.warning('Defaulting start_date to None.')
            start_date = None
        run_ETLpipeline(start_date)
        return jsonify({'message': 'Refresh successful'}), 200
    except Exception as e:
        logger.error(f'Refresh failed: {e}')
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
