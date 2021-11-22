from flask import (
    Blueprint,
    jsonify,
)
from urllib.parse import quote_plus as urlquote
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from app import csrf
from app.service.forms import (
    GetMysqlTableMetadataForm,
)

service = Blueprint('service', __name__)


@service.route('/get-mysql-table-metadata', methods=['POST'])
@csrf.exempt
def get_mysql_table_metadata():
    """
    Get db table metadata

    Format:
    TableMetadata = {
        columns: List[ColumnMetadata]
        num_rows: int
        last_updated: str
        name: str
        schema: str
        database: str
    }

    ColumnMetadata = {
        col_name: str
        col_type: str
        col_full_type: str
    }
    """

    # Step 1: validate input form
    form = GetMysqlTableMetadataForm()

    # handle bad request: invalid input
    if not form.validate_on_submit():
        err_list = []
        for fieldName, errorMessages in form.errors.items():
            for err in errorMessages:
                print(f"Invalid Input. Field [{fieldName}]: {err}")
                err_list.append(f"Invalid Input. Field [{fieldName}]: {err}")

        print('Invalid input form.')
        if len(err_list):
            return jsonify({'errors': err_list, "success": False}), 400
        return jsonify({'errors': "Oops.. please try again later", "success": False}), 500

    # Step 2: create connection
    db_name = form.db_name.data.strip()
    connect_string = create_connect_string(form)
    engine = create_engine(connect_string)

    # Step 3: get table metadata
    try:
        sql = f"""
        SELECT 
        A.TABLE_SCHEMA, A.TABLE_NAME, A.TABLE_TYPE, A.TABLE_ROWS, A.UPDATE_TIME,
        B.TABLE_SCHEMA, B.TABLE_NAME, B.COLUMN_NAME, B.DATA_TYPE, B.COLUMN_TYPE, B.COLUMN_KEY
        FROM information_schema.tables A
        JOIN information_schema.columns B
        ON A.TABLE_NAME = B.TABLE_NAME
        WHERE A.TABLE_SCHEMA = "{db_name}" and B.TABLE_SCHEMA = "{db_name}" and A.TABLE_TYPE = 'BASE TABLE';
        """
        result = engine.execute(sql)

        # Step 4: parse table data & return
        table_meta_data = parse_metadata(result)
        # print(f"result is: {table_meta_data}")
        return jsonify({'success': True, 'data': list(table_meta_data.values())})
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        return jsonify({'errors': error, "success": False}), 400


###################################################################################
# Helper Functions
###################################################################################

def create_connect_string(form):
    # get params from form
    username = form.username.data.strip()
    password = form.password.data.strip()
    url = form.url.data.strip()
    port = form.port.data if form.port.data > 0 else 3306
    db_name = form.db_name.data.strip()

    # NOTE: parse bad password (may contain special characters)
    return "mysql+pymysql://%s:%s@%s:%s/%s" % (username, urlquote(password), url, port, db_name)


def parse_metadata(data):
    table_meta_data = {}
    for row in data:
        d = {}
        # row.items() returns an array like [(key0, value0), (key1, value1)]
        for column, value in row.items():
            # build up the dictionary
            d = {**d, **{column: value}}

        # add row to metadata
        if d["TABLE_NAME"] in table_meta_data:
            # add to columns
            table_meta_data[d["TABLE_NAME"]]["columns"].append({
                "col_name": d["COLUMN_NAME"],
                "col_type": d["DATA_TYPE"],
                "col_full_type": d["COLUMN_TYPE"],
            })
        else:
            table_meta_data[d["TABLE_NAME"]] = {
                "columns": [{
                    "col_name": d["COLUMN_NAME"],
                    "col_type": d["DATA_TYPE"],
                    "col_full_type": d["COLUMN_TYPE"],
                }],
                "num_rows": d["TABLE_ROWS"],
                "last_updated": d["UPDATE_TIME"],
                "name": d["TABLE_NAME"],
                "schema": d["TABLE_SCHEMA"],
                "database": d["TABLE_SCHEMA"],
            }

    return table_meta_data