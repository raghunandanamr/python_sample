from datetime import datetime, date
from typing import List, Dict, Any, Callable
import mysql.connector


# Define possible predicates
def contains(field_value: Any, rule_value: Any) -> bool:
    print("contains")
    return rule_value in field_value

def not_equals(field_value: Any, rule_value: Any) -> bool:
    print("not equal")
    return field_value != rule_value

def less_than(field_value: Any, rule_value: Any) -> bool:
    print("less than")
    return field_value < rule_value

PREDICATE_FUNCTIONS = {
    "contains": contains,
    "not_equals": not_equals,
    "less_than": less_than,
}

class Rule:
    def __init__(self, field: str, predicate: str, value: Any):
        self.field = field
        self.predicate = predicate
        self.value = value

    def applies_to(self, email: Dict[str, Any]) -> bool:
        field_value = email.get(self.field)
        if field_value is None:
            return False  # Field not found in email
        predicate_function = PREDICATE_FUNCTIONS[self.predicate]
        return predicate_function(field_value, self.value)

class RuleSet:
    def __init__(self, rules: List[Rule], match_all: bool):
        self.rules = rules
        self.match_all = match_all

    def applies_to(self, email: Dict[str, Any]) -> bool:
        if self.match_all:
            return all(rule.applies_to(email) for rule in self.rules)
        else:
            return any(rule.applies_to(email) for rule in self.rules)

    def fetch_emails():
        conn = mysql.connector.connect(host='localhost', user='root', password='root', database='sample_python')
        cursor = conn.cursor(dictionary=True)

        try:
            query = "SELECT * FROM emails"
            cursor.execute(query)
            rows = cursor.fetchall()
            rules = [
                Rule("From", "contains", ".com"),
                Rule("To", "contains", ".com"),
                Rule("Subject", "contains", "Unfold"),
                Rule("Date", "less_than", datetime.strptime('2025-01-01',"%Y-%m-%d").date()),
            ]
            rule_set = RuleSet(rules, match_all=True)
            for row in rows:
                print("subject:", 'Unfold' in row['subject'])
                print("from:", ".com" in row["from_email"])
                print("to:", ".com" in row["to_mail"])
                email = {
                    "From": row['from_email'],
                    "To": row["to_mail"],
                    "Subject": row['subject'],
                    "Date": row['date']

                }
                print("Date Comparison:" , row['date'] < datetime.strptime('2025-01-01',"%Y-%m-%d").date()  )
                if rule_set.applies_to(email):
                    print("Email matches the rules", email)
                else:
                    print("Email does not match the rules", email)

        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            cursor.close()
            conn.close()
if __name__ == "__main__":
    RuleSet.fetch_emails()