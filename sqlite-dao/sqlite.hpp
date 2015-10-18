/*
	Author:	Easton
	Date:	2013/03/13
*/

#pragma once
#include <boost/variant.hpp>
#include <vector>
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/thread.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/exception/all.hpp>
#include <boost/format.hpp>
#include <boost/assign.hpp>
#include <boost/foreach.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <custom/usefultypes.hpp>
#include <custom/exceptions.hpp>
#include <custom/codepage.hpp>
#include <custom/compact_archive.hpp>
#include <functional>
#include <regex>
#include <boost/integer.hpp>
#include "sqlite3.h"
#pragma comment(lib, "sqlite3.lib")

using namespace std;

#define	Values	sqlite_hsd::mapped_values<sqlite_hsd::value_t>()
#define	TRANSACTION_SCOPE(d)			auto trans = (d).transaction_scope()
namespace sqlite_hsd
{
	typedef boost::variant<nullptr_t, int64_t, double, wstring, vector<char>> __value_t;
	class value_t : __value_t
	{
		template<class Type>
		struct is_compatible
		{
			enum {value = is_convertible<Type, int64_t>::value || is_convertible<Type, double>::value || is_convertible<Type, wstring>::value || is_convertible<Type, vector<char>>::value};
		};
		struct value_from_string
		{
			__value_t operator()(const string& t)
			{
				return codepage::acp_to_unicode(t);
			}
		};
		struct value_from_integer
		{
			__value_t operator()(const int64_t t)
			{
				return t;
			}
		};
		struct value_from_value
		{
			const __value_t& operator()(const __value_t& t)
			{
				return t;
			}
		};
		struct value_from_value_t
		{
			const __value_t& operator()(const value_t& t)
			{
				return (const __value_t&)t;
			}
		};
		template<class Type>
		struct value_from_other_type
		{
			__value_t operator()(const Type& t)
			{
				static_assert(!is_same<Type, value_t>::value && !is_same<Type, __value_t>::value, "unsupported value type");
				return serialize_chunk(t);
			}
		};
		template<class Type>
		struct value_from_directly
		{
			__value_t operator()(const Type& t)
			{
				return t;
			}
		};
		template<class Type>
		struct value_from
		{
			__value_t operator()(const Type& t)
			{
				typedef
					boost::mpl::if_c<is_same<Type, value_t>::value, value_from_value_t,
					boost::mpl::if_c<is_same<Type, __value_t>::value, value_from_value,
					boost::mpl::if_c<boost::is_integral<Type>::value, value_from_integer,
					boost::mpl::if_c<is_convertible<Type, string>::value, value_from_string,
					boost::mpl::if_c<is_compatible<Type>::value, value_from_directly<Type>, value_from_other_type<Type>
					>::type
					>::type
					>::type
					>::type
					>::type convert_type;
				return convert_type()(t);
			}
		};

		struct value_to_string
		{
			string operator()(const value_t& v)
			{
				return codepage::unicode_to_acp(v.to<wstring>());
			}
		};
		struct value_to_integer
		{
			int64_t operator()(const value_t& v)
			{
				if(v.type() == typeid(int64_t)) return get<int64_t>(v);
				else if(v.type() == typeid(double)) return (int64_t)get<double>(v);
				else if(v.type() == typeid(wstring)) return boost::lexical_cast<int64_t, wstring>(get<wstring>(v));
				else if(v.type() == typeid(vector<char>)) return deserialize_chunk<int64_t>(get<vector<char>>(v));
				else throw exception2() << error_wtext(L"invalid conversion");
			}
		};
		struct value_to_double
		{
			double operator()(const value_t& v)
			{
				if(v.type() == typeid(double)) return get<double>(v);
				else if(v.type() == typeid(int64_t)) return (double)get<int64_t>(v);
				else if(v.type() == typeid(wstring)) return boost::lexical_cast<double, wstring>(get<wstring>(v));
				else if(v.type() == typeid(vector<char>)) return deserialize_chunk<double>(get<vector<char>>(v));
				else throw exception2() << error_wtext(L"invalid conversion");
			}
		};
		struct value_to_wstring
		{
			wstring operator()(const value_t& v)
			{
				if(typeid(wstring) == v.type()) return get<wstring>(v);
				else if(typeid(int64_t) == v.type()) return boost::lexical_cast<wstring, int64_t>(get<int64_t>(v));
				else if(typeid(double) == v.type()) return boost::lexical_cast<wstring, double>(get<double>(v));
				else if(typeid(vector<char>) == v.type()) return deserialize_chunk<wstring>(get<vector<char>>(v));
				else throw exception2() << error_wtext(L"invalid conversion");
			}
		};
		struct value_to_vector
		{
			vector<char> operator()(const value_t& v)
			{
				if(typeid(vector<char>) == v.type()) return get<vector<char>>(v);
				else throw exception2() << error_wtext(L"invalid conversion");
			}
		};
		template<class Type>
		struct value_to_other_type
		{
			Type operator()(const value_t& v)
			{
				static_assert(!is_same<Type, value_t>::value && !is_same<Type, __value_t>::value, "unsupported value");
				return deserialize_chunk<Type>(get<vector<char>>(v));
			}
		};

		template<class Type>
		struct value_to
		{
			Type operator()(const value_t& v)
			{
				typedef
					boost::mpl::if_c<is_convertible<string, Type>::value, value_to_string,
					boost::mpl::if_c<boost::is_integral<Type>::value, value_to_integer,
					boost::mpl::if_c<boost::is_convertible<double, Type>::value, value_to_double,
					boost::mpl::if_c<boost::is_convertible<wstring, Type>::value, value_to_wstring,
					boost::mpl::if_c<boost::is_convertible<vector<char>, Type>::value, value_to_vector,
					value_to_other_type<Type>
					>::type
					>::type
					>::type
					>::type
				>::type convert_type;
				return (const Type&)convert_type()(v);
			}
		};
	public:
		template<class Type> value_t(const Type& t) : __value_t(value_from<Type>()(t)) {}
		value_t(const value_t& t) : __value_t((const __value_t&)t) {}
		template<class Type> const value_t& operator =(const Type& v)
		{
			this->swap(value_from<Type>()(v));
			return *this;
		}
		const value_t& operator = (const value_t& v)
		{
			(__value_t&)*this = (const __value_t&)v;
			return *this;
		}

		value_t() {}
		template<class Type> operator Type ()const {return value_to<Type>()(*this);}
		vector<char> to_blob()const {return std::move(value_to<vector<char>>()(*this));}
		wstring to_wstring()const {return std::move(value_to<wstring>()(*this));}
		string to_string()const {return std::move(value_to<string>()(*this));}
		bool to_bool()const {return to<int64_t>() ? true : false;}

		__declspec(property(get = to_blob)) vector<char> blob;
		__declspec(property(get = to_wstring)) wstring wtext;
		__declspec(property(get = to_string)) string text;
		__declspec(property(get = to_bool)) bool boolean;

		bool empty()const {return typeid(nullptr_t) == type();}
		const std::type_info& type()const {return __super::type();}
		template<class Type> Type to()const {return value_to<Type>()(*this);}
	};

	template<typename ValueType>
	struct mapped_values : public custom::value_map_t<ValueType>
	{
		mapped_values& operator()(const wstring& key, const ValueType& value)
		{
			(*this)[key] = (const __value_t&)value;
			return *this;
		}
		mapped_values& operator()(const string& key, const ValueType& value)
		{
			return operator()(codepage::acp_to_unicode(key), value);
		}
	};
	template<int N>
	struct mapped_values<const wchar_t[N]> : public mapped_values<wstring> {};
	typedef mapped_values<value_t> mapped_table;

	class table
	{
		friend class dao;
	public:
		class record
		{
			friend class table;
		private:
			vector<value_t>			m_values;
			const table*			m_owner;

		public:
			record(const table* owner) : m_owner(owner)
			{
				m_values.assign(m_owner->column_number(), value_t());
			}
		public:
			value_t& operator[](int column) {return m_values[column];}
			const value_t& operator[](int column)const {return m_values[column];}
			const value_t& operator[](const wstring& column)const
			{
				static const value_t s_empty_value;
				wstring name = boost::to_lower_copy(column);
				auto itr = m_owner->m_column_names.find(name);
				if(m_owner->m_column_names.end() == itr) return s_empty_value;
				return operator[](itr->second);
			}
			const value_t& operator [](const string& column)const {return operator[](codepage::acp_to_unicode(column));}
		};

	private:
		map<wstring, int>			m_column_names;
		vector<record>				m_records;

	protected:
		void _add_column(const wstring& name)
		{
			auto name_l = boost::to_lower_copy(name);
			if(m_column_names.end() != m_column_names.find(name_l)) return;
			auto column = m_column_names.size();
			m_column_names[name_l] = (int)column;
		}
		void _add_record()
		{
			m_records.push_back(record(this));
		}

	public:
		void clear(bool clear_column_names = true)
		{
			m_records.clear();
			if(clear_column_names) m_column_names.clear();
		}
		record& operator [](int row) {return m_records[row];}
		const record& operator [](int row)const {return m_records[row];}

	public:
		long column_number()const {return (long)m_column_names.size();}
		long row_number()const {return (long)m_records.size();}
		wstring column_name(size_t column)const
		{
			if(m_column_names.size() <= column) return L"";
			for(auto itr = m_column_names.begin(); m_column_names.end() != itr; ++itr)
				if(itr->second == column) return itr->first;
			return L"";
		}
	};

	class command
	{
	private:
		wstring									m_command_text;
		typedef custom::value_map_t<value_t>	CommandKeyValuePair;
		CommandKeyValuePair						m_variants;

	public:
		command() : m_command_text(L"") {}
		command(const wstring& cmdtext) : m_command_text(cmdtext) {}
		void set_cmd_text(const wstring& cmdtext) {m_command_text = cmdtext;}
		const wstring& get_cmd_text()const {return m_command_text;}
		const value_t& get_bind_value(const wstring& key)const {return m_variants.find(key)->second;}
		void bind_parameter(const wstring& key, const value_t& variant) {m_variants[key] = variant;}
	};
	class dao : boost::noncopyable, public std::enable_shared_from_this<dao>
	{
		friend class transaction;
	protected:
		class transaction : noncopyable
		{
		private:
			boost::recursive_mutex::scoped_lock	ul;
			dao&						db;
		public:
			transaction(dao* d) : ul(d->m_connection_mutex), db(*d)
			{
				db.begin_transaction();
			}
			~transaction()
			{
				try
				{
					db.commit_transaction();
				}
				catch(...)
				{
					db.rollback_transaction();
				}
			}
		};
	public:
		dao() {}
		virtual ~dao() {close();}

		void open(const boost::filesystem::path& datasource, const wstring& password = L"")
		{
			DeclareSection(m_connection_mutex);
			auto open_connection = [](const boost::filesystem::path& source)->std::shared_ptr<sqlite3>
				{
					sqlite3* connection = nullptr;
					sqlite3_open(codepage::unicode_to_utf8(source.wstring()).c_str(), &connection);
					return std::shared_ptr<sqlite3>(connection, sqlite3_close);
				};
			m_connection = open_connection(datasource);

			if(false == password.empty()) 
			{
				auto utf8_pwd = codepage::unicode_to_utf8(password);
				sqlite3_key(m_connection.get(), utf8_pwd.c_str(), (int)utf8_pwd.size());
			}
			m_datasource = datasource;
		}
		void close()
		{
			DeclareSection(m_connection_mutex);
			m_connection = std::shared_ptr<sqlite3>();
		}
		bool is_open()const {return m_connection != nullptr;}
		boost::filesystem::path source()const {return m_datasource;}
		size_t execute(const command& cmd, table* table = nullptr, uint64_t start = 0, uint64_t count = -1)
		{
			std::shared_ptr<sqlite3_stmt>	stmt;
			int				index;
			int				column_count;

			DeclareSection(m_connection_mutex);

			if(false == is_open()) _commit_error("data base is not open");
			try{
				auto text = codepage::unicode_to_utf8(cmd.get_cmd_text());
				if(0 != start || -1 != count)
				{
					text += (boost::format(" limit %1%, %2%") % start % count).str();
				}

				stmt = [&text](std::shared_ptr<sqlite3> connection)->std::shared_ptr<sqlite3_stmt>
					{
						sqlite3_stmt* stmt;
						sqlite3_prepare(connection.get(), text.c_str(), -1, &stmt, nullptr);
						return std::shared_ptr<sqlite3_stmt>(stmt, sqlite3_finalize);
					}(m_connection);
				if(!stmt)
					_commit_error();
				column_count = sqlite3_bind_parameter_count(stmt.get());

				for(index = 1; index <= column_count; ++index)
				{
					string text = sqlite3_bind_parameter_name(stmt.get(), index);
					text.erase(0, 1);
					auto value = cmd.get_bind_value(codepage::utf8_to_unicode(text));

					if(typeid(wstring) == value.type()) sqlite3_bind_text(stmt.get(), index, codepage::unicode_to_utf8(value).c_str(), -1, SQLITE_TRANSIENT);
					else if(typeid(int64_t) == value.type()) sqlite3_bind_int64(stmt.get(), index, value);
					else if(typeid(double) == value.type()) sqlite3_bind_double(stmt.get(), index, value);
					else if(typeid(vector<char>) == value.type())
					{
						auto vec = std::move(value.to_blob());
						if(vec.size() != 0)
							sqlite3_bind_blob(stmt.get(), index, &vec[0], (int)vec.size(), SQLITE_TRANSIENT);
						else
							sqlite3_bind_null(stmt.get(), index);
					}
					else if(value.empty()) sqlite3_bind_null(stmt.get(), index);
				}

				int				column_count;
				long			i, j;
				int				ret;

				column_count = sqlite3_column_count(stmt.get());

				if(nullptr != table)
				{
					table->clear();
					for(i = 0; i < column_count; ++i)
						table->_add_column(codepage::utf8_to_unicode(sqlite3_column_name(stmt.get(), i)));
				}
				i = 0;
				while(SQLITE_ROW == (ret = sqlite3_step(stmt.get())))
				{
					if(nullptr != table)
					{
						table->_add_record();
						for(j = 0; j < column_count; ++j)
						{
							switch(sqlite3_column_type(stmt.get(), j))
							{
							case SQLITE_INTEGER:
								(*table)[i][j] = sqlite3_column_int64(stmt.get(), j);
								break;
							case SQLITE_FLOAT:
								(*table)[i][j] = sqlite3_column_double(stmt.get(), j);
								break;
							case SQLITE_TEXT:
								(*table)[i][j] = codepage::utf8_to_unicode((const char*)sqlite3_column_text(stmt.get(), j));
								break;
							case SQLITE_BLOB:
								{
									vector<char> blob(sqlite3_column_bytes(stmt.get(), j));
									memcpy(&blob[0], sqlite3_column_blob(stmt.get(), j), blob.size());
									(*table)[i][j] = std::move(blob);
								}
								break;
							case SQLITE_NULL:
								(*table)[i][j] = value_t();
								break;
							}
						}
					}
					++i;
				}

				if(column_count && SQLITE_DONE != ret)
				{
					_commit_error();
				}
				return sqlite3_changes(m_connection.get());
			}
			catch(const exception2& e)
			{
				throw e;
			}
			catch(...)
			{
				commit_error(L"unknown error while executing command in sqlite_hsd.");
			}
		}
		std::auto_ptr<transaction> transaction_scope()
		{
			return std::auto_ptr<transaction>(new transaction(this));
		}
		void get_table_info(const wstring& table_name, table* t)
		{
			table dt;
			wchar_t text_buffer[1024];

			wsprintf(text_buffer, TEXT("select sql from sqlite_master where name = '%s'"), table_name);
			const_cast<dao*>(this)->execute(wstring(text_buffer), &dt, 0, -1);

			t->clear();
			t->_add_column(L"name");
			t->_add_column(L"type");
			t->_add_column(L"length");
			t->_add_column(L"precision");
			t->_add_column(L"restriction");
			if(dt.row_number())
			{
				auto text = dt[0][0].wtext;
				std::wregex reg(L"\\[\\w+\\].*");
				wsregex_iterator itr(text.begin(), text.end(), reg);
				wsregex_iterator end;
				std::for_each(itr, end, [](const wsmatch& it)
				{
					//it[0] = line text
					//set table trait record
					assert(false);
				});
			}
		}
		void begin_transaction()
		{
			m_connection_mutex.lock();
			_exec("begin transaction;");
		}
		void commit_transaction()
		{
			_exec("commit transaction;");
			m_connection_mutex.unlock();
		}
		void rollback_transaction()
		{
			_exec("rollback transaction;");
			m_connection_mutex.unlock();
		}
		void abort()
		{
			sqlite3_interrupt(m_connection.get());
			m_connection_mutex.unlock();
		}
		void create_table(const wstring& name, const wstring& keys)
		{
			execute(L"create table [" + name + L"](" + keys + L")");
		}
	private:

		inline void _exec(const string& sql)
		{
			char*	error = nullptr;
			if(SQLITE_OK != sqlite3_exec(m_connection.get(), sql.c_str(), 0, 0, &error))
				_commit_error(error);
		}
		inline void _commit_error(const string& error)
		{
			commit_error(codepage::acp_to_unicode(error));
		}
		inline void _commit_error()
		{
			auto text = sqlite3_errmsg(m_connection.get());
			_commit_error(text);
		}
	private:
		std::shared_ptr<sqlite3> m_connection;
		boost::recursive_mutex m_connection_mutex;
		boost::filesystem::path m_datasource;
	};
	class table_adapter
	{
	private:
		std::shared_ptr<dao> database;
		wstring table;
		list<wstring> columns;
		uint64_t start;
		uint64_t count;
		wstring where_clause;

	public:
		table_adapter(std::shared_ptr<dao> _d, const wstring& t) : table(t), start(0), count(-1), database(_d) {}
		table_adapter(std::shared_ptr<dao> _d, const string& t) : table(codepage::acp_to_unicode(t)), start(0), count(-1), database(_d) {}
		template<typename CharType>
		table_adapter(const basic_string<CharType>& t, const boost::filesystem::path& source, const basic_string<CharType>& password = basic_string<CharType>())
			: table(codepage::_to_unicode<CP_ACP>(t)), start(0), count(-1), database(std::make_shared<dao>())
		{
			database->open(source, codepage::_to_unicode<CP_ACP>(password));
			if(false == database->is_open())
				commit_error(L"cannot open the database.");
		}
		table_adapter operator ()(const wstring& column)const
		{
			auto other = *this;
			other.columns.push_back(column);
			return other;
		}
		table_adapter operator ()(const string& column)const {return operator()(codepage::acp_to_unicode(column));}
		table_adapter operator [](const wstring& clause)const
		{
			auto other = *this;
			if(0 == other.where_clause.size())
				other.where_clause = L"where " + clause;
			else
				other.where_clause += L" and " + clause;
			return other;
		}
		table_adapter operator [](const string& clause)const {return operator[](codepage::acp_to_unicode(clause));}
		table_adapter operator ()(uint64_t start, uint64_t count)const
		{
			auto other = *this;
			other.start = start;
			other.count = count;
			return other;
		}
		template<typename ValueType>
		const table_adapter& operator += (const custom::value_map_t<ValueType>& values)const
		{
			execute_insert(values);
			return *this;
		}
		template<typename ValueType>
		const table_adapter& operator -= (const custom::value_map_t<ValueType>& values)const
		{
			execute_delete(values);
			return *this;
		}
		template<typename ValueType>
		const table_adapter& operator |= (const custom::value_map_t<ValueType>& values)const
		{
			execute_update(values, true);
			return *this;
		}
		template<typename ValueType>
		const table_adapter& operator ^= (const custom::value_map_t<ValueType>& values)const
		{
			execute_update(values, false);
			return *this;
		}
		template<class Type>
		const table_adapter& operator << (const Type& a)
		{
			return *this += a;
		}
		const table_adapter& change_table(const wstring& t)
		{
			table = t;
			return *this;
		}
		const table_adapter& operator >> (sqlite_hsd::table& t)const
		{
			wstring sql = (boost::wformat(L"select %1% from [%2%] %3%") % select_columns() % table % where_clause).str();
			database->execute(sql, &t, start, count);
			return *this;
		}
		void create_table(const wstring& keys)
		{
			try{
				database->create_table(table, keys);
			}
			catch(...)
			{
			}
		}
		int64_t rows()
		{
			sqlite_hsd::table t;
			database->execute((boost::wformat(L"select count(*) from [%1%]") % table).str(), &t);
			return t[0][0];
		}
		void create_table(const string& keys) {create_table(codepage::acp_to_unicode(keys));}
		std::shared_ptr<dao> get_database() {return database;}
	private:
		wstring select_columns()const
		{
			wstring keys;
			if(0 == columns.size()) keys = L"*";
			else
				BOOST_FOREACH(auto& c, columns)
				{
					if(0 != keys.size()) keys += L",";
					keys += c;
				}
			return keys;
		}
		template<typename ValueType>
		void execute_insert(const custom::value_map_t<ValueType>& values)const
		{
			wstring keys, qkeys;
			command cmd;
			boost::wformat fmt(L"insert into [%1%](%2%) values(%3%)");

			BOOST_FOREACH(auto& it, values)
			{
				if(0 != columns.size() && false == name_in_columns(it.first))
					continue;
				if(keys.size())
				{
					keys += L",";
					qkeys += L",";
				}
				keys += it.first;
				qkeys += wstring(L":") + it.first;
				cmd.bind_parameter(it.first, it.second);
			}
			cmd.set_cmd_text((fmt % table % keys % qkeys).str());
			database->execute(cmd);
		}
		template<typename ValueType>
		void execute_delete(const custom::value_map_t<ValueType>& values)const
		{
			wstring conds;
			command cmd;
			boost::wformat fmt(L"delete from [%1%] %2%");
			conds = where_clause;
			BOOST_FOREACH(auto& it, values)
			{
				if(conds.size()) conds += L" and ";
				else conds = L" where ";
				conds += it.first + L"=:" + it.first;
				cmd.bind_parameter(it.first, it.second);
			}
			cmd.set_cmd_text((fmt % table % conds).str());
			database->execute(cmd);
		}
		template<typename ValueType>
		void execute_update(const custom::value_map_t<ValueType>& values, bool insertIfNonExistent)const
		{
			wstring conds;
			command cmd;
			boost::wformat fmt(L"update [%1%] set %2% %3%");
			BOOST_FOREACH(auto& it, values)
			{
				if(0 != columns.size() && false == name_in_columns(it.first))
					continue;
				if(conds.size()) conds += L", ";
				conds += it.first + L"=:" + it.first;
				cmd.bind_parameter(it.first, it.second);
			}
			cmd.set_cmd_text((fmt % table % conds % where_clause).str());
			if(0 == database->execute(cmd) && insertIfNonExistent)
				execute_insert(values);
		}
		bool name_in_columns(const wstring& name)const
		{
			BOOST_FOREACH(auto& it, columns)
				if(boost::iequals(it, name)) return true;
			return false;
		}
	};
}