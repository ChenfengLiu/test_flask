from flask_wtf import FlaskForm
from wtforms.fields import (
    PasswordField,
    StringField,
    IntegerField,
    SubmitField,
)
from wtforms.validators import InputRequired, Length


class GetMysqlTableMetadataForm(FlaskForm):
    # disable csrf (just for this API)
    class Meta:
        csrf = False

    """
    url: required, string
    username: required, string
    password: required, string
    port: default 3306, integer
    db_name: required, string
    """
    url = StringField('url', validators=[InputRequired(), Length(1, 1000)])
    username = StringField('username', validators=[InputRequired(), Length(1, 255)])
    password = PasswordField('Password', validators=[InputRequired(), Length(1, 255)])
    port = IntegerField('port', validators=[], default=3306)
    db_name = StringField('db_name', validators=[InputRequired(), Length(1, 255)])
    submit = SubmitField('Get mysql table metadata form')
