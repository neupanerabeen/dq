from pydeequ.suggestions import *
import re
import json

class DQ_Suggestion():
	def __init__(self, data, spark):
		self.__data = data
		self.__spark = spark
		self.__suggestionsReport = []

	def __get_params(self, rule_constraints):
		try:
			_params = re.findall( r'\(.*\)', rule_constraints)[0]
			_params = _params.strip("(").strip(")").split(",")
			_params = [_p.strip() for _p in _params]
			_params = [_params[0].strip("\""), ", ".join(_params[1:])]
			return _params
		except Exception as e:
			print(e)
			return []

	def __handler_isUnique(self, rule):
		_code = get_params(rule["code_for_constraint"])
		try:
			_col = _code[0]
		except IndexError as _ke:
			_col = None
		try:
			_params = _code[1]
		except IndexError as _ke:
			_params = {}
		return "is_unique", _col, {}

	def __handler_isNonNegative(self, rule):
		_code = get_params(rule["code_for_constraint"])
		try:
			_col = _code[0]
		except IndexError as _ke:
			_col = None
		try:
			_params = _code[1]
		except IndexError as _ke:
			_params = {}
		return "has_min_value", _col, {"value":0}

	def __handler_isComplete(self, rule):
		_code = get_params(rule["code_for_constraint"])
		try:
			_col = _code[0]
		except IndexError as _ke:
			_col = None
		try:
			_params = _code[1]
		except IndexError as _ke:
			_params = {}  
		return ("is_null", _col, {"values":_params})

	def __handler_hasCompleteness(self, rule):
		_code = get_params(rule["code_for_constraint"])
		try:
			_col = _code[0]
		except IndexError as _ke:
			_col = None
		try:
			_params = _code[1].split(" ")[-1]
		except IndexError as _ke:
			_params = {}  
		return ("is_null", _col, _params)


	def __handler_isContainedIn(self, rule):
		_code = get_params(rule["code_for_constraint"])
		try:
			_col = _code[0]
		except IndexError as _ke:
			_col = None
		try:
			_params = json.loads(_code[1].split("]")[0]+"]")
		except IndexError as _ke:
			_params = {}  
		return ("contains", _col, {"values":_params})


	def __handler_hasDataType(self, rule):
		_code = get_params(rule["code_for_constraint"])
		try:
			_col = _code[0]
		except IndexError as _ke:
			_col = None
		try:
			_params = _code[1].split(" ")[-1]
		except IndexError as _ke:
			_params = {}  
		return ("is_type", _col, {"type":datatype_map[_params]})


	def __get_from_deequ(self):
		suggestionResult = ConstraintSuggestionRunner(self.__spark).onData(self.__data).addConstraintRule(DEFAULT()).run()
		suggestedRules = []
		for rule in suggestionResult["constraint_suggestions"]:
			_curr_rule = rule["code_for_constraint"].strip(".")
			_curr_rule = "__handler_"+re.findall(r'^.[a-zA-Z]+', _curr_rule)[0]
			try:
				# (_rule, col, params) = rules_handler_map[_curr_rule](rule)
				(_rule, col, params) = rule()
				suggestedRules.append({
					"rule":_rule,
					"col":col,
					"params":params
				})
			except KeyError as _ke:
				print("Rule map not found")
				raise _ke
			except Exception as e:
				print(rule)
				raise e
		return suggestedRules

	def __get_algo(self):
		return []

	def run(self):
		deequ_resp = self.__get_from_deequ()
		algo = self.__get_algo()
		self.__suggestionsReport = self.__suggestionsReport.append(deequ_resp).append(algo)



	def get_rules(self):
		return self.__suggestionsReport% 
