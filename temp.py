from DatabaseAdaptor import DatabaseAdaptor, DatabaseOperator
from models import Action, ActionType, Stats
import uuid


adaptor = DatabaseAdaptor(DatabaseOperator())
adaptor.writeNewUser(str(uuid.uuid4()), 'mint')