import pandas as pd
import numpy as np
import json
import re
import time
import dateutil
from pyspark.sql.functions import *
from pyspark.sql.types import *

class DQ():
	def __init__(self, **kwargs):
		self.__tmpColName = "___tmp_col_name___"
		self.__tmp_col_value = ""
		self.__dump_reason_title = "__reason__"
		self.__dump_reason_delimiter = "\n"
		self.__current_rule = ""
		self.task_summary = []

		try:
			data = kwargs["data"]
			self.set_data(data)
		except Exception as e:
			raise e

		try:
			self.rules = kwargs["rules"]
		except:
			self.rules = []

		try:
			self.spark = kwargs["sparkSession"]
		except:
			try:          
				self.spark = spark
			except:
				raise Exception("SparkSession needs to be passed")


	# ----------------------------------------------------------------------
	#  Helper method's section
	# ----------------------------------------------------------------------


	def __append_to_summary(self, valid_count):
		row = [
			self.__current_rule,
			self.__current_col,
			valid_count,
			self.__total_rows - valid_count,
		]
		self.task_summary.append(row)



	# ----------------------------------------------------------------------
	#  Rules definition section
	# ----------------------------------------------------------------------
		

	def _rule_has_max_length(self, config = {}):
		params = config['params']
		col = config['col']
		params = int(params["value"])
		self._data = self._data.withColumn(self.__tmpColName, when( length(self._data[col]) > params , "column :{} has length more than {}".format(col,params) ).otherwise(self.__tmp_col_value))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter),self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)



	def _rule_has_min_length(self, config = {}):
		params 	= config['params']
		col = config['col']
		params = int(params["value"])
		self._data = self._data.withColumn(self.__tmpColName, when( length(self._data[col]) < params , "column :{} has less than {}".format(col,params) ).otherwise(self.__tmp_col_value))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter),self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)


	def _rule_has_max_value(self, config = {}):
		params = config['params']
		col = config['col']
		params = float(params["value"])
		self._data = self._data.withColumn(self.__tmpColName, when( self._data[col]  >= params , "column:{} is more than {}".format(col, params) ).otherwise(self.__tmp_col_value))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter),self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)


	def _rule_has_min_value(self, config):
		params = config['params']
		col = config['col']
		params = int(params["value"])
		self._data = self._data.withColumn(self.__tmpColName, when( self._data[col] <= params , "column :{} is less than {}".format(col,params) ).otherwise(self.__tmp_col_value))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter),self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)


	def _rule_has_regex(self, config):
		params = config['params']
		regex = params["regex"]
		col = config['col']
		self._data = self._data.withColumn(self.__tmpColName, when(regexp_extract( self._data[col], regex, 0) =="", "column :{} failed regex match with pattern:{}".format(col, params, col) ).otherwise(self.__tmp_col_value))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter), self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)

# 

	def _rule_is_ssn(self, config):
		params = config['params']
		regex = "^(?!666|000|9\\d{2})\\d{3}-(?!00)\\d{2}-(?!0{4})\\d{4}$"
		col = config['col']
		self._data = self._data.withColumn(self.__tmpColName, when(regexp_extract( self._data[col], regex, 0) =="", "column :{} failed regex match with pattern:{}".format(col, params, col) ).otherwise(self.__tmp_col_value))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter), self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)

	def _rule_is_email(self, config):
		params = config['params']
		regex = "^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$"
		col = config['col']
		self._data = self._data.withColumn(self.__tmpColName, when(regexp_extract( self._data[col], regex, 0) =="", "column :{} failed regex match with pattern:{}".format(col, params, col) ).otherwise(self.__tmp_col_value))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter), self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)

	def _rule_is_url(self, config):
		params = config['params']
		regex = "(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})"
		col = config['col']
		self._data = self._data.withColumn(self.__tmpColName, when(regexp_extract( self._data[col], regex, 0) =="", "column :{} failed regex match with pattern:{}".format(col, params, col) ).otherwise(self.__tmp_col_value))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter), self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)

	def _rule_is_ipv4(self, config):
		params = config['params']
		regex = "^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
		col = config['col']
		self._data = self._data.withColumn(self.__tmpColName, when(regexp_extract( self._data[col], regex, 0) =="", "column :{} failed regex match with pattern:{}".format(col, params, col) ).otherwise(self.__tmp_col_value))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter), self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)

	def _rule_is_null(self, config ={}):
		col = config['col']
		self._data = self._data.withColumn(self.__tmpColName, when(self._data[col].isNull(), "column :{} has null value".format(col) ).otherwise(self.__tmp_col_value))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter), self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)

	def _rule_is_non_negative(self, config):
		col = config['col']
		params = config['params']
		self._data = self._data.withColumn(self.__tmpColName, when(self._data[col] < 0, "column :{} has negative value".format(col) ).otherwise(self.__tmp_col_value))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter), self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)

	def _rule_contains(self, config ={}):
		params = config['params']
		value_options = params["values"] 
		col = config['col']
		self._data = self._data.withColumn(self.__tmpColName, when(self._data[col].isin(value_options), self.__tmp_col_value ).otherwise("column :{} contains other values".format(col)))
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter), self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)

# 
	def _rule_is_unique(self,config={}): #is primary key-multiple columns
		col=config['col']
		grouped_df = self._data.groupBy(col).count()		
		self._data = self._data.join(grouped_df, col, "left")
		self._data = self._data.withColumn(self.__tmpColName, when(self._data["count"] == 1, self.__tmp_col_value).otherwise("Duplicate entry for column : {}".format(col)))
		self._data = self._data.drop("count")
		valid_count = self._data.filter(self._data[self.__tmpColName] == self.__tmp_col_value).count()
		self._data = self._data.withColumn(self.__dump_reason_title,  concat(self.__dump_reason_title, lit(self.__dump_reason_delimiter), self.__tmpColName)).drop(self.__tmpColName)
		self.__append_to_summary(valid_count)

	def _rule_word_similarity(self, config={}):
		from word_similarity import WordSimilarity
		col = config["col"]
		try:
			score_column=config["params"]["score_column"]
		except KeyError as _ke:
			score_column = False
		try:
			lookup_file=config["params"]["lookup_file"]
		except KeyError as _ke:
			raise Exception("Lookup file not passed!")
		try:
			lookup_column=config["params"]["lookup_column"]
		except KeyError as _ke:
			raise Exception("Lookup file not passed!")
		try:
			dest_column=config["params"]["dest_column"]
		except KeyError as _ke:
			dest_column = col
		try:
			threshold=config["params"]["threshold"]
		except KeyError as _ke:
			raise Exception("threshold not passed!")
		lookup = self.spark.read.format(lookup_file.split(".")[-1]).option("header", True).load(lookup_file).select(lookup_column).toPandas()[lookup_column]
		lookup_bdcast = self.spark.sparkContext.broadcast(lookup)
		def _internal_def(inp_string):
			res = WordSimilarity.similarity(lookup_bdcast.value, pd.Series([inp_string,]) )
			confidence = 0
			_name = inp_string
			try:
				confidence = res.iloc[0]["conf"]
			except:
				pass
			if confidence >= threshold:
				try:
					_name = res["matched_name"][0]
				except:
					pass
			return [_name, str(confidence)]
# 		_dq_word_mapper_udf = udf(lambda x: _internal_def(x), StringType())
		_dq_word_mapper_udf = udf(lambda x: _internal_def(x), ArrayType(StringType()))
		self._data = self._data.withColumn(dest_column, _dq_word_mapper_udf(col) )
		if score_column != False:
			self._data = self._data.withColumn("score_column", self._data[dest_column][1] )
		self._data = self._data.withColumn(dest_column, self._data[dest_column][0] )
		self.columns.append(dest_column)


	def _rule_spell_corrector(self, config={}):
		from word_correction import WordCorrection
		col = config["col"]
		try:
			dest_column=config["params"]["dest_column"]
			self.columns.append(dest_column)
		except KeyError as _ke:
			dest_column = col
		from pyspark.sql.functions import udf
		from pyspark.sql.types import StringType
		_dq_word_corrector_udf = udf(lambda x: WordCorrection.suggest_correct_word(x), StringType())
		self._data = self._data.withColumn(dest_column, _dq_word_corrector_udf(col) )

	def _rule_word_seperator(self, config={}):
		from word_space import WordSpace
		col = config["col"]
		try:
			dest_column=config["params"]["dest_column"]
			self.columns.append(dest_column)
		except KeyError as _ke:
			dest_column = col
		from pyspark.sql.functions import udf
		from pyspark.sql.types import StringType
		_dq_word_separator_udf = udf(lambda x: " ".join(WordSpace.infer_spaces(x)), StringType())
		self._data = self._data.withColumn(dest_column, _dq_word_separator_udf(col) )

	# def _is_type(self, config):
	# 	def fuzzy(value, _type):
	# 		_type = _type.lower()
	# 		if _type == "int":
	# 			try:
	# 				_tmp_df = self._data.withColumn("__test_dtypes__", self._data[])
	# 				return isinstance(int(value), int)
	# 			except Exception as e:
	# 				return False

	# 		elif _type == "float":
	# 			try:
	# 				return isinstance(float(value), float)
	# 			except Exception as e:
	# 				return False	
			
	# 		elif _type == "string":
	# 			try:
	# 				return isinstance(str(value), str)
	# 			except Exception as e:
	# 				return False

	# 		elif _type == "date":
	# 			try:
	# 				# if its integer, than fail this. because dateutil lib accpets int as valid date
	# 				if isinstance(int(value), int):
	# 					return False
	# 			except:
	# 				pass

	# 			try:
	# 				# if the datataype is already Timestamp, pass this
	# 				if isinstance(value, pd._libs.tslibs.timestamps.Timestamp):
	# 					return True
	# 			except:
	# 				pass

	# 			try:
	# 				if isinstance(value, str):
	# 					_val = dateutil.parser.parse(value)
	# 					return True
	# 			except:
	# 				return False
	# 			return False
	# 		else:
	# 			raise Exception("Cannot identify data type")
		
	# 	col = config['col']
	# 	params = config['params']
	# 	_config_dtype = params["type"].lower()
	# 	try:
	# 		fuzzy_parse_val = params["fuzzy_parse"]
	# 		if type(fuzzy_parse_val) is str:
	# 			fuzzy_parse_val = fuzzy_parse_val.lower()
	# 			fuzzy_parse = fuzzy_parse_val in ["true"]
	# 		elif type(fuzzy_parse_val) is bool:
	# 			fuzzy_parse = fuzzy_parse_val
	# 		else:
	# 			fuzzy_parse =False
	# 	except:
	# 		fuzzy_parse = False

	# 	df_length = self._data.shape[0]
	# 	if _config_dtype == "object":
	# 		self.__append_to_summary(df_length)
	# 		return
	# 	_input_dtypes = self._data.dtypes
	# 	col_input_dtypes = _input_dtypes[col].name.lower()
	# 	if fuzzy_parse == True:
	# 		tmp = self._data.apply(lambda x: x[self.__dump_reason_title] if pd.isnull(x[col]) else (x[self.__dump_reason_title] if fuzzy(x[col], _config_dtype) == True else x[self.__dump_reason_title].append({"col":self.__current_col, "rule":self.__current_rule})), axis = 1)
	# 		self.__append_to_summary(tmp.count(None))
	# 		return
	# 	else:
	# 		if _config_dtype != col_input_dtypes:
	# 			self._data.apply(lambda x: x[self.__dump_reason_title].append({"col":self.__current_col, "rule":self.__current_rule}), axis = 1)
	# 			self.__append_to_summary(0)
	# 			return
	# 		else:
	# 			self.__append_to_summary(df_length)
	# 			return


	# def _is_type(self, config):
	# 	def fuzzy(value, _type):
	# 		_type = _type.lower()
	# 		if _type == "int":
	# 			try:
	# 				return isinstance(int(value), int)
	# 			except Exception as e:
	# 				return False

	# 		elif _type == "float":
	# 			try:
	# 				return isinstance(float(value), float)
	# 			except Exception as e:
	# 				return False	
			
	# 		elif _type == "string":
	# 			try:
	# 				return isinstance(str(value), str)
	# 			except Exception as e:
	# 				return False

	# 		elif _type == "date":
	# 			try:
	# 				# if its integer, than fail this. because dateutil lib accpets int as valid date
	# 				if isinstance(int(value), int):
	# 					return False
	# 			except:
	# 				pass

	# 			try:
	# 				# if the datataype is already Timestamp, pass this
	# 				if isinstance(value, pd._libs.tslibs.timestamps.Timestamp):
	# 					return True
	# 			except:
	# 				pass

	# 			try:
	# 				if isinstance(value, str):
	# 					_val = dateutil.parser.parse(value)
	# 					return True
	# 			except:
	# 				return False
	# 			return False
	# 		else:
	# 			raise Exception("Cannot identify data type")
		
	# 	col = config['col']
	# 	params = config['params']
	# 	_config_dtype = params["type"].lower()
	# 	try:
	# 		fuzzy_parse_val = params["fuzzy_parse"]
	# 		if type(fuzzy_parse_val) is str:
	# 			fuzzy_parse_val = fuzzy_parse_val.lower()
	# 			fuzzy_parse = fuzzy_parse_val in ["true"]
	# 		elif type(fuzzy_parse_val) is bool:
	# 			fuzzy_parse = fuzzy_parse_val
	# 		else:
	# 			fuzzy_parse =False
	# 	except:
	# 		fuzzy_parse = False

	# 	df_length = self._data.shape[0]
	# 	if _config_dtype == "object":
	# 		self.__append_to_summary(df_length)
	# 		return
	# 	_input_dtypes = self._data.dtypes
	# 	col_input_dtypes = _input_dtypes[col].name.lower()
	# 	if fuzzy_parse == True:
	# 		tmp = self._data.apply(lambda x: x[self.__dump_reason_title] if pd.isnull(x[col]) else (x[self.__dump_reason_title] if fuzzy(x[col], _config_dtype) == True else x[self.__dump_reason_title].append({"col":self.__current_col, "rule":self.__current_rule})), axis = 1)
	# 		self.__append_to_summary(tmp.count(None))
	# 		return
	# 	else:
	# 		if _config_dtype != col_input_dtypes:
	# 			self._data.apply(lambda x: x[self.__dump_reason_title].append({"col":self.__current_col, "rule":self.__current_rule}), axis = 1)
	# 			self.__append_to_summary(0)
	# 			return
	# 		else:
	# 			self.__append_to_summary(df_length)
	# 			return



	# ----------------------------------------------------------------------
	#  Public exposed APIs
	# ----------------------------------------------------------------------


	def set_rules(self, rules):
		self.rules = rules
		return self


	def set_data(self, df):
		self._data = df
		self._data = self._data.withColumn(self.__dump_reason_title, lit("")).cache()
		self.columns = df.columns
		self.__total_rows = df.count()
		return self


	def get_report(self):
		return self.task_summary

	def dirty_data(self):
		return self._data.filter(self._data[self.__dump_reason_title] != self.__tmp_col_value)
	
	def clean_data(self):
		# self._data = self._data.drop(self.__dump_reason_title)
		return self._data.filter(self._data[self.__dump_reason_title] == self.__tmp_col_value).drop(self.__dump_reason_title)
		
	def close(self):
		# self._data = self._data.drop(self.__dump_reason_title)
		return self._data.unpersist()


	def run(self):
		for config in self.rules:
			rule = config["rule"]
			col = config["col"]
			params = config["params"]
			if type(col) is str:
				if col not in self.columns:
					msg = "Column '{}' not found in data. ".format(col)
					raise KeyError(msg)
			else:
				for i in col:
					if i not in self.columns:
						msg = "Column '{}' not found in data. ".format(i)
						raise KeyError(msg)
			try:
				_curr_rule = "_rule_"+rule
				_rule = self.__getattribute__(_curr_rule)
			except Exception as e:
				msg = "Rule '{}' not found.".format(rule)
				raise e
# 				raise KeyError(msg)
			self.__current_rule = rule
			self.__current_col = col
			try:
				res = _rule(config)
				if res == False:
					msg = "Error while handling {}".format(config)
					raise Exception(msg)
			except Exception as e:
				raise e
		self._data = self._data.withColumn(self.__dump_reason_title, when( regexp_replace(self._data[self.__dump_reason_title ], self.__dump_reason_delimiter, "")=="", "").otherwise( regexp_replace(self._data[self.__dump_reason_title ], self.__dump_reason_delimiter+"+", self.__dump_reason_delimiter) ) )
		return self%  
