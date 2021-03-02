%language "C++"
%skeleton "lalr1.cc"
%no-lines
%locations
%define api.namespace { nebula }
%define parser_class_name { GraphParser }
%lex-param { nebula::GraphScanner& scanner }
%parse-param { nebula::GraphScanner& scanner }
%parse-param { std::string &errmsg }
%parse-param { nebula::Sentence** sentences }
%parse-param { nebula::graph::QueryContext* qctx }

%code requires {
#include <iostream>
#include <sstream>
#include <string>
#include <cstddef>
#include "parser/ExplainSentence.h"
#include "parser/SequentialSentences.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/expression/AttributeExpression.h"
#include "common/expression/LabelAttributeExpression.h"
#include "common/expression/VariableExpression.h"
#include "common/expression/CaseExpression.h"
#include "common/expression/TextSearchExpression.h"
#include "common/expression/PredicateExpression.h"
#include "common/expression/ListComprehensionExpression.h"
#include "common/expression/AggregateExpression.h"

#include "common/expression/ReduceExpression.h"
#include "util/ParserUtil.h"
#include "context/QueryContext.h"
#include "util/SchemaUtil.h"
#include "util/ParserUtil.h"
#include "context/QueryContext.h"

namespace nebula {

class GraphScanner;

}

static constexpr size_t MAX_ABS_INTEGER = 9223372036854775808ULL;

}

%code {
    #include "GraphScanner.h"
    static int yylex(nebula::GraphParser::semantic_type* yylval,
                     nebula::GraphParser::location_type *yylloc,
                     nebula::GraphScanner& scanner);

    void ifOutOfRange(const int64_t input,
                      const nebula::GraphParser::location_type& loc);
}

%union {
    bool                                    boolval;
    int64_t                                 intval;
    double                                  doubleval;
    std::string                            *strval;
    nebula::meta::cpp2::ColumnTypeDef      *type;
    nebula::Expression                     *expr;
    nebula::Sentence                       *sentence;
    nebula::Sentence                       *sentences;
    nebula::SequentialSentences            *seq_sentences;
    nebula::ExplainSentence                *explain_sentence;
    nebula::ColumnSpecification            *colspec;
    nebula::ColumnSpecificationList        *colspeclist;
    nebula::ColumnNameList                 *column_name_list;
    nebula::StepClause                     *step_clause;
    nebula::StepClause                     *find_path_upto_clause;
    nebula::FromClause                     *from_clause;
    nebula::ToClause                       *to_clause;
    nebula::VertexIDList                   *vid_list;
    nebula::NameLabelList                  *name_label_list;
    nebula::OverEdge                       *over_edge;
    nebula::OverEdges                      *over_edges;
    nebula::OverClause                     *over_clause;
    nebula::WhereClause                    *where_clause;
    nebula::WhereClause                    *lookup_where_clause;
    nebula::WhenClause                     *when_clause;
    nebula::YieldClause                    *yield_clause;
    nebula::YieldColumns                   *yield_columns;
    nebula::YieldColumn                    *yield_column;
    nebula::VertexTagList                  *vertex_tag_list;
    nebula::VertexTagItem                  *vertex_tag_item;
    nebula::PropertyList                   *prop_list;
    nebula::ValueList                      *value_list;
    nebula::VertexRowList                  *vertex_row_list;
    nebula::VertexRowItem                  *vertex_row_item;
    nebula::EdgeRowList                    *edge_row_list;
    nebula::EdgeRowItem                    *edge_row_item;
    nebula::UpdateList                     *update_list;
    nebula::UpdateItem                     *update_item;
    nebula::ArgumentList                   *argument_list;
    nebula::SpaceOptList                   *space_opt_list;
    nebula::SpaceOptItem                   *space_opt_item;
    nebula::AlterSchemaOptList             *alter_schema_opt_list;
    nebula::AlterSchemaOptItem             *alter_schema_opt_item;
    nebula::RoleTypeClause                 *role_type_clause;
    nebula::AclItemClause                  *acl_item_clause;
    nebula::SchemaPropList                 *create_schema_prop_list;
    nebula::SchemaPropItem                 *create_schema_prop_item;
    nebula::SchemaPropList                 *alter_schema_prop_list;
    nebula::SchemaPropItem                 *alter_schema_prop_item;
    nebula::OrderFactor                    *order_factor;
    nebula::OrderFactors                   *order_factors;
    nebula::meta::cpp2::ConfigModule        config_module;
    nebula::meta::cpp2::ListHostType       list_host_type;
    nebula::ConfigRowItem                  *config_row_item;
    nebula::EdgeKey                        *edge_key;
    nebula::EdgeKeys                       *edge_keys;
    nebula::EdgeKeyRef                     *edge_key_ref;
    nebula::GroupClause                    *group_clause;
    nebula::HostList                       *host_list;
    nebula::HostAddr                       *host_item;
    nebula::ZoneNameList                   *zone_name_list;
    std::vector<int32_t>                   *integer_list;
    nebula::InBoundClause                  *in_bound_clause;
    nebula::OutBoundClause                 *out_bound_clause;
    nebula::BothInOutClause                *both_in_out_clause;
    ExpressionList                         *expression_list;
    MapItemList                            *map_item_list;
    MatchPath                              *match_path;
    MatchNode                              *match_node;
    MatchNodeLabel                         *match_node_label;
    MatchNodeLabelList                     *match_node_label_list;
    MatchEdge                              *match_edge;
    MatchEdgeProp                          *match_edge_prop;
    MatchEdgeTypeList                      *match_edge_type_list;
    MatchReturn                            *match_return;
    ReadingClause                          *reading_clause;
    MatchClauseList                        *match_clause_list;
    MatchStepRange                         *match_step_range;
    nebula::meta::cpp2::IndexFieldDef      *index_field;
    nebula::IndexFieldList                 *index_field_list;
    CaseList                               *case_list;
    nebula::TextSearchArgument             *text_search_argument;
    nebula::TextSearchArgument             *base_text_search_argument;
    nebula::TextSearchArgument             *fuzzy_text_search_argument;
    nebula::meta::cpp2::FTClient           *text_search_client_item;
    nebula::TSClientList                   *text_search_client_list;
}

/* destructors */
%destructor {} <sentences>
%destructor {} <boolval> <intval> <doubleval> <type> <config_module> <integer_list> <list_host_type>
%destructor { delete $$; } <*>

/* keywords */
%token KW_BOOL KW_INT8 KW_INT16 KW_INT32 KW_INT64 KW_INT KW_FLOAT KW_DOUBLE
%token KW_STRING KW_FIXED_STRING KW_TIMESTAMP KW_DATE KW_TIME KW_DATETIME
%token KW_GO KW_AS KW_TO KW_USE KW_SET KW_FROM KW_WHERE KW_ALTER
%token KW_MATCH KW_INSERT KW_VALUES KW_YIELD KW_RETURN KW_CREATE KW_VERTEX
%token KW_EDGE KW_EDGES KW_STEPS KW_OVER KW_UPTO KW_REVERSELY KW_SPACE KW_DELETE KW_FIND
%token KW_TAG KW_TAGS KW_UNION KW_INTERSECT KW_MINUS
%token KW_NO KW_OVERWRITE KW_IN KW_DESCRIBE KW_DESC KW_SHOW KW_HOST KW_HOSTS KW_PART KW_PARTS KW_ADD
%token KW_PARTITION_NUM KW_REPLICA_FACTOR KW_CHARSET KW_COLLATE KW_COLLATION KW_VID_TYPE
%token KW_ATOMIC_EDGE
%token KW_DROP KW_REMOVE KW_SPACES KW_INGEST KW_INDEX KW_INDEXES
%token KW_IF KW_NOT KW_EXISTS KW_WITH
%token KW_COUNT KW_COUNT_DISTINCT KW_SUM KW_AVG KW_MAX KW_MIN KW_STD KW_BIT_AND KW_BIT_OR KW_BIT_XOR KW_COLLECT KW_COLLECT_SET
%token KW_BY KW_DOWNLOAD KW_HDFS KW_UUID KW_CONFIGS KW_FORCE
%token KW_GET KW_DECLARE KW_GRAPH KW_META KW_STORAGE
%token KW_TTL KW_TTL_DURATION KW_TTL_COL KW_DATA KW_STOP
%token KW_FETCH KW_PROP KW_UPDATE KW_UPSERT KW_WHEN
%token KW_ORDER KW_ASC KW_LIMIT KW_OFFSET KW_ASCENDING KW_DESCENDING
%token KW_DISTINCT KW_ALL KW_OF
%token KW_BALANCE KW_LEADER KW_RESET KW_PLAN
%token KW_SHORTEST KW_PATH KW_NOLOOP
%token KW_IS KW_NULL KW_DEFAULT
%token KW_SNAPSHOT KW_SNAPSHOTS KW_LOOKUP
%token KW_JOBS KW_JOB KW_RECOVER KW_FLUSH KW_COMPACT KW_REBUILD KW_SUBMIT KW_STATS KW_STATUS
%token KW_BIDIRECT
%token KW_USER KW_USERS KW_ACCOUNT
%token KW_PASSWORD KW_CHANGE KW_ROLE KW_ROLES
%token KW_GOD KW_ADMIN KW_DBA KW_GUEST KW_GRANT KW_REVOKE KW_ON
%token KW_OUT KW_BOTH KW_SUBGRAPH
%token KW_EXPLAIN KW_PROFILE KW_FORMAT
%token KW_CONTAINS
%token KW_STARTS KW_ENDS
%token KW_UNWIND KW_SKIP KW_OPTIONAL
%token KW_CASE KW_THEN KW_ELSE KW_END
%token KW_GROUP KW_ZONE KW_GROUPS KW_ZONES KW_INTO
%token KW_LISTENER KW_ELASTICSEARCH
%token KW_AUTO KW_FUZZY KW_PREFIX KW_REGEXP KW_WILDCARD
%token KW_TEXT KW_SEARCH KW_CLIENTS KW_SIGN KW_SERVICE KW_TEXT_SEARCH
%token KW_ANY KW_SINGLE KW_NONE
%token KW_REDUCE

/* symbols */
%token L_PAREN R_PAREN L_BRACKET R_BRACKET L_BRACE R_BRACE COMMA
%token PIPE ASSIGN
%token DOT DOT_DOT COLON QM SEMICOLON L_ARROW R_ARROW AT
%token ID_PROP TYPE_PROP SRC_ID_PROP DST_ID_PROP RANK_PROP INPUT_REF DST_REF SRC_REF

/* token type specification */
%token <boolval> BOOL
%token <intval> INTEGER
%token <doubleval> DOUBLE
%token <strval> STRING VARIABLE LABEL IPV4

%type <strval> name_label unreserved_keyword agg_function predicate_name
%type <expr> expression
%type <expr> property_expression
%type <expr> vertex_prop_expression
%type <expr> edge_prop_expression
%type <expr> input_prop_expression
%type <expr> var_prop_expression
%type <expr> generic_case_expression
%type <expr> conditional_expression
%type <expr> vid_ref_expression
%type <expr> vid
%type <expr> function_call_expression
%type <expr> uuid_expression
%type <expr> list_expression
%type <expr> set_expression
%type <expr> map_expression
%type <expr> container_expression
%type <expr> subscript_expression
%type <expr> attribute_expression
%type <expr> case_expression
%type <expr> predicate_expression
%type <expr> list_comprehension_expression
%type <expr> reduce_expression
%type <expr> compound_expression
%type <expr> aggregate_expression
%type <expr> text_search_expression
%type <argument_list> argument_list opt_argument_list
%type <type> type_spec
%type <step_clause> step_clause
%type <from_clause> from_clause
%type <vid_list> vid_list
%type <over_edge> over_edge
%type <over_edges> over_edges
%type <over_clause> over_clause
%type <where_clause> where_clause
%type <lookup_where_clause> lookup_where_clause
%type <when_clause> when_clause
%type <yield_clause> yield_clause
%type <yield_columns> yield_columns
%type <yield_column> yield_column
%type <vertex_tag_list> vertex_tag_list
%type <vertex_tag_item> vertex_tag_item
%type <prop_list> prop_list
%type <value_list> value_list
%type <vertex_row_list> vertex_row_list
%type <vertex_row_item> vertex_row_item
%type <edge_row_list> edge_row_list
%type <edge_row_item> edge_row_item
%type <update_list> update_list
%type <update_item> update_item
%type <space_opt_list> space_opt_list
%type <space_opt_item> space_opt_item
%type <alter_schema_opt_list> alter_schema_opt_list
%type <alter_schema_opt_item> alter_schema_opt_item
%type <create_schema_prop_list> create_schema_prop_list opt_create_schema_prop_list
%type <create_schema_prop_item> create_schema_prop_item
%type <alter_schema_prop_list> alter_schema_prop_list
%type <alter_schema_prop_item> alter_schema_prop_item
%type <order_factor> order_factor
%type <order_factors> order_factors
%type <config_module> config_module_enum
%type <list_host_type> list_host_type
%type <config_row_item> show_config_item get_config_item set_config_item
%type <edge_key> edge_key
%type <edge_keys> edge_keys
%type <edge_key_ref> edge_key_ref
%type <to_clause> to_clause
%type <find_path_upto_clause> find_path_upto_clause
%type <group_clause> group_clause
%type <host_list> host_list
%type <host_item> host_item
%type <integer_list> integer_list
%type <in_bound_clause> in_bound_clause
%type <out_bound_clause> out_bound_clause
%type <both_in_out_clause> both_in_out_clause
%type <expression_list> expression_list
%type <map_item_list> map_item_list
%type <case_list> when_then_list
%type <expr> case_condition
%type <expr> case_default

%type <match_path> match_path_pattern
%type <match_path> match_path
%type <match_node> match_node
%type <match_node_label> match_node_label
%type <match_node_label_list> match_node_label_list
%type <match_edge> match_edge
%type <match_edge_prop> match_edge_prop
%type <match_return> match_return
%type <expr> match_skip
%type <expr> match_limit
%type <strval> match_alias
%type <match_edge_type_list> match_edge_type_list
%type <match_edge_type_list> opt_match_edge_type_list
%type <reading_clause> unwind_clause with_clause match_clause reading_clause
%type <match_clause_list> reading_clauses reading_with_clause reading_with_clauses
%type <match_step_range> match_step_range
%type <order_factors> match_order_by
%type <text_search_argument> text_search_argument
%type <base_text_search_argument> base_text_search_argument
%type <fuzzy_text_search_argument> fuzzy_text_search_argument
%type <text_search_client_item> text_search_client_item
%type <text_search_client_list> text_search_client_list

%type <intval> legal_integer unary_integer rank port job_concurrency

%type <colspec> column_spec
%type <colspeclist> column_spec_list
%type <column_name_list> column_name_list
%type <zone_name_list> zone_name_list

%type <role_type_clause> role_type_clause
%type <acl_item_clause> acl_item_clause

%type <name_label_list> name_label_list
%type <index_field> index_field
%type <index_field_list> index_field_list opt_index_field_list

%type <sentence> maintain_sentence
%type <sentence> create_space_sentence describe_space_sentence drop_space_sentence
%type <sentence> create_tag_sentence create_edge_sentence
%type <sentence> alter_tag_sentence alter_edge_sentence
%type <sentence> drop_tag_sentence drop_edge_sentence
%type <sentence> describe_tag_sentence describe_edge_sentence
%type <sentence> create_tag_index_sentence create_edge_index_sentence
%type <sentence> drop_tag_index_sentence drop_edge_index_sentence
%type <sentence> describe_tag_index_sentence describe_edge_index_sentence
%type <sentence> rebuild_tag_index_sentence rebuild_edge_index_sentence
%type <sentence> add_group_sentence drop_group_sentence desc_group_sentence
%type <sentence> add_zone_into_group_sentence drop_zone_from_group_sentence
%type <sentence> add_zone_sentence drop_zone_sentence desc_zone_sentence
%type <sentence> add_host_into_zone_sentence drop_host_from_zone_sentence
%type <sentence> create_snapshot_sentence drop_snapshot_sentence
%type <sentence> add_listener_sentence remove_listener_sentence list_listener_sentence

%type <sentence> admin_job_sentence
%type <sentence> create_user_sentence alter_user_sentence drop_user_sentence change_password_sentence
%type <sentence> show_sentence

%type <sentence> mutate_sentence
%type <sentence> insert_vertex_sentence insert_edge_sentence
%type <sentence> delete_vertex_sentence delete_edge_sentence
%type <sentence> update_vertex_sentence update_edge_sentence
%type <sentence> download_sentence ingest_sentence

%type <sentence> traverse_sentence
%type <sentence> go_sentence match_sentence lookup_sentence find_path_sentence get_subgraph_sentence
%type <sentence> group_by_sentence order_by_sentence limit_sentence
%type <sentence> fetch_sentence fetch_vertices_sentence fetch_edges_sentence
%type <sentence> set_sentence piped_sentence assignment_sentence
%type <sentence> yield_sentence use_sentence

%type <sentence> grant_sentence revoke_sentence
%type <sentence> set_config_sentence get_config_sentence balance_sentence
%type <sentence> process_control_sentence return_sentence
%type <sentence> sentence
%type <seq_sentences> seq_sentences
%type <explain_sentence> explain_sentence
%type <sentences> sentences
%type <sentence> sign_in_text_search_service_sentence sign_out_text_search_service_sentence

%type <boolval> opt_if_not_exists
%type <boolval> opt_if_exists
%type <boolval> opt_with_properites

%left QM COLON
%left KW_OR KW_XOR
%left KW_AND
%right KW_NOT
%left EQ NE LT LE GT GE REG KW_IN KW_NOT_IN KW_CONTAINS KW_NOT_CONTAINS KW_STARTS_WITH KW_ENDS_WITH KW_NOT_STARTS_WITH KW_NOT_ENDS_WITH
%left PLUS MINUS
%left STAR DIV MOD
%right NOT
%nonassoc UNARY_PLUS
%nonassoc UNARY_MINUS
%nonassoc CASTING

%start sentences

%%

name_label
    : LABEL { $$ = $1; }
    | unreserved_keyword { $$ = $1; }
    ;

name_label_list
    : name_label {
        $$ = new NameLabelList();
        $$->add($1);
    }
    | name_label_list COMMA name_label {
        $1->add($3);
        $$ = $1;
    }
    ;

legal_integer
    : INTEGER {
        ifOutOfRange($1, @1);
        $$ = $1;
    }
    ;

/**
 * TODO(dutor) Tweak the scanner to keep the original text along the unreserved keywords
 */
unreserved_keyword
    : KW_SPACE              { $$ = new std::string("space"); }
    | KW_VALUES             { $$ = new std::string("values"); }
    | KW_HOST               { $$ = new std::string("host"); }
    | KW_HOSTS              { $$ = new std::string("hosts"); }
    | KW_SPACES             { $$ = new std::string("spaces"); }
    | KW_USER               { $$ = new std::string("user"); }
    | KW_USERS              { $$ = new std::string("users"); }
    | KW_PASSWORD           { $$ = new std::string("password"); }
    | KW_ROLE               { $$ = new std::string("role"); }
    | KW_ROLES              { $$ = new std::string("roles"); }
    | KW_GOD                { $$ = new std::string("god"); }
    | KW_ADMIN              { $$ = new std::string("admin"); }
    | KW_DBA                { $$ = new std::string("dba"); }
    | KW_GUEST              { $$ = new std::string("guest"); }
    | KW_GROUP              { $$ = new std::string("group"); }
    | KW_COUNT              { $$ = new std::string("count"); }
    | KW_SUM                { $$ = new std::string("sum"); }
    | KW_AVG                { $$ = new std::string("avg"); }
    | KW_MAX                { $$ = new std::string("max"); }
    | KW_MIN                { $$ = new std::string("min"); }
    | KW_STD                { $$ = new std::string("std"); }
    | KW_BIT_AND            { $$ = new std::string("bit_and"); }
    | KW_BIT_OR             { $$ = new std::string("bit_or"); }
    | KW_BIT_XOR            { $$ = new std::string("bit_xor"); }
    | KW_COLLECT            { $$ = new std::string("collect"); }
    | KW_COLLECT_SET        { $$ = new std::string("collect_set"); }
    | KW_PATH               { $$ = new std::string("path"); }
    | KW_DATA               { $$ = new std::string("data"); }
    | KW_LEADER             { $$ = new std::string("leader"); }
    | KW_UUID               { $$ = new std::string("uuid"); }
    | KW_JOB                { $$ = new std::string("job"); }
    | KW_JOBS               { $$ = new std::string("jobs"); }
    | KW_BIDIRECT           { $$ = new std::string("bidirect"); }
    | KW_FORCE              { $$ = new std::string("force"); }
    | KW_PART               { $$ = new std::string("part"); }
    | KW_PARTS              { $$ = new std::string("parts"); }
    | KW_DEFAULT            { $$ = new std::string("default"); }
    | KW_CONFIGS            { $$ = new std::string("configs"); }
    | KW_ACCOUNT            { $$ = new std::string("account"); }
    | KW_HDFS               { $$ = new std::string("hdfs"); }
    | KW_PARTITION_NUM      { $$ = new std::string("partition_num"); }
    | KW_REPLICA_FACTOR     { $$ = new std::string("replica_factor"); }
    | KW_CHARSET            { $$ = new std::string("charset"); }
    | KW_COLLATE            { $$ = new std::string("collate"); }
    | KW_COLLATION          { $$ = new std::string("collation"); }
    | KW_ATOMIC_EDGE        { $$ = new std::string("atomic_edge"); }
    | KW_TTL_DURATION       { $$ = new std::string("ttl_duration"); }
    | KW_TTL_COL            { $$ = new std::string("ttl_col"); }
    | KW_SNAPSHOT           { $$ = new std::string("snapshot"); }
    | KW_SNAPSHOTS          { $$ = new std::string("snapshots"); }
    | KW_GRAPH              { $$ = new std::string("graph"); }
    | KW_META               { $$ = new std::string("meta"); }
    | KW_STORAGE            { $$ = new std::string("storage"); }
    | KW_ALL                { $$ = new std::string("all"); }
    | KW_ANY                { $$ = new std::string("any"); }
    | KW_SINGLE             { $$ = new std::string("single"); }
    | KW_NONE               { $$ = new std::string("none"); }
    | KW_REDUCE             { $$ = new std::string("reduce"); }
    | KW_SHORTEST           { $$ = new std::string("shortest"); }
    | KW_NOLOOP             { $$ = new std::string("noloop"); }
    | KW_COUNT_DISTINCT     { $$ = new std::string("count_distinct"); }
    | KW_CONTAINS           { $$ = new std::string("contains"); }
    | KW_STARTS             { $$ = new std::string("starts"); }
    | KW_ENDS               { $$ = new std::string("ends"); }
    | KW_VID_TYPE           { $$ = new std::string("vid_type"); }
    | KW_LIMIT              { $$ = new std::string("limit"); }
    | KW_SKIP               { $$ = new std::string("skip"); }
    | KW_OPTIONAL           { $$ = new std::string("optional"); }
    | KW_OFFSET             { $$ = new std::string("offset"); }
    | KW_FORMAT             { $$ = new std::string("format"); }
    | KW_PROFILE            { $$ = new std::string("profile"); }
    | KW_BOTH               { $$ = new std::string("both"); }
    | KW_OUT                { $$ = new std::string("out"); }
    | KW_SUBGRAPH           { $$ = new std::string("subgraph"); }
    | KW_THEN               { $$ = new std::string("then"); }
    | KW_ELSE               { $$ = new std::string("else"); }
    | KW_END                { $$ = new std::string("end"); }
    | KW_INTO               { $$ = new std::string("into"); }
    | KW_GROUPS             { $$ = new std::string("groups"); }
    | KW_ZONE               { $$ = new std::string("zone"); }
    | KW_ZONES              { $$ = new std::string("zones"); }
    | KW_LISTENER           { $$ = new std::string("listener"); }
    | KW_ELASTICSEARCH      { $$ = new std::string("elasticsearch"); }
    | KW_STATS              { $$ = new std::string("stats"); }
    | KW_STATUS             { $$ = new std::string("status"); }
    | KW_AUTO               { $$ = new std::string("auto"); }
    | KW_FUZZY              { $$ = new std::string("fuzzy"); }
    | KW_PREFIX             { $$ = new std::string("prefix"); }
    | KW_REGEXP             { $$ = new std::string("regexp"); }
    | KW_WILDCARD           { $$ = new std::string("wildcard"); }
    | KW_TEXT               { $$ = new std::string("text"); }
    | KW_SEARCH             { $$ = new std::string("search"); }
    | KW_CLIENTS            { $$ = new std::string("clients"); }
    | KW_SIGN               { $$ = new std::string("sign"); }
    | KW_SERVICE            { $$ = new std::string("service"); }
    | KW_TEXT_SEARCH        { $$ = new std::string("text_search"); }
    | KW_RESET              { $$ = new std::string("reset"); }
    | KW_PLAN               { $$ = new std::string("plan"); }
    ;

agg_function
    : KW_COUNT              { $$ = new std::string("COUNT"); }
    | KW_SUM                { $$ = new std::string("SUM"); }
    | KW_AVG                { $$ = new std::string("AVG"); }
    | KW_MAX                { $$ = new std::string("MAX"); }
    | KW_MIN                { $$ = new std::string("MIN"); }
    | KW_STD                { $$ = new std::string("STD"); }
    | KW_BIT_AND            { $$ = new std::string("BIT_AND"); }
    | KW_BIT_OR             { $$ = new std::string("BIT_OR"); }
    | KW_BIT_XOR            { $$ = new std::string("BIT_XOR"); }
    | KW_COLLECT            { $$ = new std::string("COLLECT"); }
    | KW_COLLECT_SET        { $$ = new std::string("COLLECT_SET"); }
    ;

expression
    : DOUBLE {
        $$ = new ConstantExpression($1);
    }
    | STRING {
        $$ = new ConstantExpression(*$1);
        delete $1;
    }
    | BOOL {
        $$ = new ConstantExpression($1);
    }
    | KW_NULL {
        $$ = new ConstantExpression(NullType::__NULL__);
    }
    | name_label {
        $$ = new LabelExpression($1);
    }
    | VARIABLE {
        $$ = new VariableExpression($1);
    }
    | INTEGER {
        $$ = new ConstantExpression($1);
    }
    | compound_expression {
        $$ = $1;
    }
    | aggregate_expression {
        $$ = $1;
    }
    | MINUS {
        scanner.setUnaryMinus(true);
    } expression %prec UNARY_MINUS {
        $$ = new UnaryExpression(Expression::Kind::kUnaryNegate, $3);
        scanner.setUnaryMinus(false);
    }
    | PLUS expression %prec UNARY_PLUS {
        $$ = new UnaryExpression(Expression::Kind::kUnaryPlus, $2);
    }
    | NOT expression {
        $$ = new UnaryExpression(Expression::Kind::kUnaryNot, $2);
    }
    | KW_NOT expression {
        $$ = new UnaryExpression(Expression::Kind::kUnaryNot, $2);
    }
    | L_PAREN type_spec R_PAREN expression %prec CASTING {
        $$ = new TypeCastingExpression(graph::SchemaUtil::propTypeToValueType($2->type), $4);
        delete $2;
    }
    | expression STAR expression {
        $$ = new ArithmeticExpression(Expression::Kind::kMultiply, $1, $3);
    }
    | expression DIV expression {
        $$ = new ArithmeticExpression(Expression::Kind::kDivision, $1, $3);
    }
    | expression MOD expression {
        $$ = new ArithmeticExpression(Expression::Kind::kMod, $1, $3);
    }
    | expression PLUS expression {
        $$ = new ArithmeticExpression(Expression::Kind::kAdd, $1, $3);
    }
    | expression MINUS expression {
        $$ = new ArithmeticExpression(Expression::Kind::kMinus, $1, $3);
    }
    | expression LT expression {
        $$ = new RelationalExpression(Expression::Kind::kRelLT, $1, $3);
    }
    | expression GT expression {
        $$ = new RelationalExpression(Expression::Kind::kRelGT, $1, $3);
    }
    | expression LE expression {
        $$ = new RelationalExpression(Expression::Kind::kRelLE, $1, $3);
    }
    | expression GE expression {
        $$ = new RelationalExpression(Expression::Kind::kRelGE, $1, $3);
    }
    | expression REG expression {
        $$ = new RelationalExpression(Expression::Kind::kRelREG, $1, $3);
    }
    | expression KW_IN expression {
        $$ = new RelationalExpression(Expression::Kind::kRelIn, $1, $3);
    }
    | expression KW_NOT_IN expression {
        $$ = new RelationalExpression(Expression::Kind::kRelNotIn, $1, $3);
    }
    | expression KW_CONTAINS expression {
        $$ = new RelationalExpression(Expression::Kind::kContains, $1, $3);
    }
    | expression KW_NOT_CONTAINS expression {
        $$ = new RelationalExpression(Expression::Kind::kNotContains, $1, $3);
    }
    | expression KW_STARTS_WITH expression {
        $$ = new RelationalExpression(Expression::Kind::kStartsWith, $1, $3);
    }
    | expression KW_NOT_STARTS_WITH expression {
        $$ = new RelationalExpression(Expression::Kind::kNotStartsWith, $1, $3);
    }
    | expression KW_ENDS_WITH expression {
        $$ = new RelationalExpression(Expression::Kind::kEndsWith, $1, $3);
    }
    | expression KW_NOT_ENDS_WITH expression {
        $$ = new RelationalExpression(Expression::Kind::kNotEndsWith, $1, $3);
    }
    | expression EQ expression {
        $$ = new RelationalExpression(Expression::Kind::kRelEQ, $1, $3);
    }
    | expression NE expression {
        $$ = new RelationalExpression(Expression::Kind::kRelNE, $1, $3);
    }
    | expression KW_AND expression {
        $$ = new LogicalExpression(Expression::Kind::kLogicalAnd, $1, $3);
    }
    | expression KW_OR expression {
        $$ = new LogicalExpression(Expression::Kind::kLogicalOr, $1, $3);
    }
    | expression KW_XOR expression {
        $$ = new LogicalExpression(Expression::Kind::kLogicalXor, $1, $3);
    }
    | case_expression {
        $$ = $1;
    }
    | predicate_expression {
        $$ = $1;
    }
    | list_comprehension_expression {
        $$ = $1;
    }
    | reduce_expression {
        $$ = $1;
    }
    ;

compound_expression
    : L_PAREN expression R_PAREN {
        $$ = $2;
    }
    | property_expression {
        $$ = $1;
    }
    | function_call_expression {
        $$ = $1;
    }
    | container_expression {
        $$ = $1;
    }
    | subscript_expression {
        $$ = $1;
    }
    | attribute_expression {
        $$ = $1;
    }
    ;

property_expression
    : input_prop_expression {
        $$ = $1;
    }
    | vertex_prop_expression {
        $$ = $1;
    }
    | var_prop_expression {
        $$ = $1;
    }
    | edge_prop_expression {
        $$ = $1;
    }
    ;

subscript_expression
    : name_label L_BRACKET expression R_BRACKET {
        $$ = new SubscriptExpression(new LabelExpression($1), $3);
    }
    | VARIABLE L_BRACKET expression R_BRACKET {
        $$ = new SubscriptExpression(new VariableExpression($1), $3);
    }
    | compound_expression L_BRACKET expression R_BRACKET {
        $$ = new SubscriptExpression($1, $3);
    }
    ;

attribute_expression
    : name_label DOT name_label {
        $$ = new LabelAttributeExpression(new LabelExpression($1),
                                          new ConstantExpression(*$3));
        delete $3;
    }
    | compound_expression DOT name_label {
        $$ = new AttributeExpression($1, new ConstantExpression(*$3));
        delete $3;
    }
    ;

case_expression
    : generic_case_expression {
        $$ = $1;
    }
    | conditional_expression {
        $$ = $1;
    }
    ;

generic_case_expression
    : KW_CASE case_condition when_then_list case_default KW_END {
        auto expr = new CaseExpression($3);
        expr->setCondition($2);
        expr->setDefault($4);
        $$ = expr;
    }
    ;

conditional_expression
    : expression QM expression COLON expression {
        auto cases = new CaseList();
        cases->add($1, $3);
        auto expr = new CaseExpression(cases, false);
        expr->setDefault($5);
        $$ = expr;
    }
    ;

case_condition
    : %empty {
        $$ = nullptr;
    }
    | expression {
        $$ = $1;
    }
    ;

case_default
    : %empty {
        $$ = nullptr;
    }
    | KW_ELSE expression {
        $$ = $2;
    }
    ;

when_then_list
    : KW_WHEN expression KW_THEN expression {
        $$ = new CaseList();
        $$->add($2, $4);
    }
    | when_then_list KW_WHEN expression KW_THEN expression {
        $1->add($3, $5);
        $$ = $1;
    }
    ;

predicate_name
    : KW_ALL                { $$ = new std::string("all"); }
    | KW_ANY                { $$ = new std::string("any"); }
    | KW_SINGLE             { $$ = new std::string("single"); }
    | KW_NONE               { $$ = new std::string("none"); }
    ;

predicate_expression
    : predicate_name L_PAREN expression KW_IN expression KW_WHERE expression R_PAREN {
        if ($3->kind() != Expression::Kind::kLabel) {
            throw nebula::GraphParser::syntax_error(@3, "The loop variable must be a label in predicate functions");
        }
        auto &innerVar = *(static_cast<const LabelExpression *>($3)->name());
        auto *expr = new PredicateExpression($1, new std::string(innerVar), $5, $7);
        nebula::graph::ParserUtil::rewritePred(qctx, expr, innerVar);
        $$ = expr;
        delete $3;
    }
    ;

list_comprehension_expression
    : L_BRACKET expression KW_IN expression KW_WHERE expression R_BRACKET {
        if ($2->kind() != Expression::Kind::kLabel) {
            throw nebula::GraphParser::syntax_error(@2, "The loop variable must be a label in list comprehension");
        }
        auto &innerVar = *(static_cast<const LabelExpression *>($2)->name());
        auto *expr = new ListComprehensionExpression(new std::string(innerVar), $4, $6, nullptr);
        nebula::graph::ParserUtil::rewriteLC(qctx, expr, innerVar);
        $$ = expr;
        delete $2;
    }
    | L_BRACKET expression KW_IN expression PIPE expression R_BRACKET {
        if ($2->kind() != Expression::Kind::kLabel) {
            throw nebula::GraphParser::syntax_error(@2, "The loop variable must be a label in list comprehension");
        }
        auto &innerVar = *(static_cast<const LabelExpression *>($2)->name());
        auto *expr = new ListComprehensionExpression(new std::string(innerVar), $4, nullptr, $6);
        nebula::graph::ParserUtil::rewriteLC(qctx, expr, innerVar);
        $$ = expr;
        delete $2;
    }
    | L_BRACKET expression KW_IN expression KW_WHERE expression PIPE expression R_BRACKET {
        if ($2->kind() != Expression::Kind::kLabel) {
            throw nebula::GraphParser::syntax_error(@2, "The loop variable must be a label in list comprehension");
        }
        auto &innerVar = *(static_cast<const LabelExpression *>($2)->name());
        auto *expr = new ListComprehensionExpression(new std::string(innerVar), $4, $6, $8);
        nebula::graph::ParserUtil::rewriteLC(qctx, expr, innerVar);
        $$ = expr;
        delete $2;
    }
    ;

reduce_expression
    : KW_REDUCE L_PAREN name_label ASSIGN expression COMMA name_label KW_IN expression PIPE expression R_PAREN {
        auto *expr = new ReduceExpression($3, $5, $7, $9, $11);
        nebula::graph::ParserUtil::rewriteReduce(qctx, expr, *$3, *$7);
        $$ = expr;
    }
    ;

input_prop_expression
    : INPUT_REF DOT name_label {
        $$ = new InputPropertyExpression($3);
    }
    | INPUT_REF DOT STAR {
        $$ = new InputPropertyExpression(new std::string("*"));
    }
    ;

vertex_prop_expression
    : SRC_REF DOT name_label DOT name_label {
        $$ = new SourcePropertyExpression($3, $5);
    }
    | DST_REF DOT name_label DOT name_label {
        $$ = new DestPropertyExpression($3, $5);
    }
    ;

var_prop_expression
    : VARIABLE DOT name_label {
        $$ = new VariablePropertyExpression($1, $3);
    }
    | VARIABLE DOT STAR {
        $$ = new VariablePropertyExpression($1, new std::string("*"));
    }
    ;

edge_prop_expression
    : name_label DOT TYPE_PROP {
        $$ = new EdgeTypeExpression($1);
    }
    | name_label DOT SRC_ID_PROP {
        $$ = new EdgeSrcIdExpression($1);
    }
    | name_label DOT DST_ID_PROP {
        $$ = new EdgeDstIdExpression($1);
    }
    | name_label DOT RANK_PROP {
        $$ = new EdgeRankExpression($1);
    }
    ;

function_call_expression
    : LABEL L_PAREN opt_argument_list R_PAREN {
        $$ = new FunctionCallExpression($1, $3);
    }
    | KW_TIMESTAMP L_PAREN opt_argument_list R_PAREN {
        $$ = new FunctionCallExpression(new std::string("timestamp"), $3);
    }
    | KW_DATE L_PAREN opt_argument_list R_PAREN {
        $$ = new FunctionCallExpression(new std::string("date"), $3);
    }
    | KW_TIME L_PAREN opt_argument_list R_PAREN {
        $$ = new FunctionCallExpression(new std::string("time"), $3);
    }
    | KW_DATETIME L_PAREN opt_argument_list R_PAREN {
        $$ = new FunctionCallExpression(new std::string("datetime"), $3);
    }
    | KW_TAGS L_PAREN opt_argument_list R_PAREN {
        $$ = new FunctionCallExpression(new std::string("tags"), $3);
    }
    | KW_SIGN L_PAREN opt_argument_list R_PAREN {
        $$ = new FunctionCallExpression(new std::string("sign"), $3);
    }
    ;

aggregate_expression
    : agg_function L_PAREN expression R_PAREN {
        $$ = new AggregateExpression($1, $3, false/*distinct*/);
    }
    | agg_function L_PAREN KW_DISTINCT expression R_PAREN {
        $$ = new AggregateExpression($1, $4, true/*distinct*/);
    }
    | agg_function L_PAREN STAR R_PAREN {
        auto star = new ConstantExpression(std::string("*"));
        $$ = new AggregateExpression($1, star, false/*distinct*/);
    }
    | agg_function L_PAREN KW_DISTINCT STAR R_PAREN {
        auto star = new ConstantExpression(std::string("*"));
        $$ = new AggregateExpression($1, star, true/*distinct*/);
    }
    ;

uuid_expression
    : KW_UUID L_PAREN STRING R_PAREN {
        $$ = new UUIDExpression($3);
    }
    ;

opt_argument_list
    : %empty {
        $$ = nullptr;
    }
    | argument_list {
        $$ = $1;
    }
    ;

argument_list
    : expression {
        $$ = new ArgumentList();
        std::unique_ptr<Expression> arg;
        arg.reset($1);
        $$->addArgument(std::move(arg));
    }
    | argument_list COMMA expression {
        $$ = $1;
        std::unique_ptr<Expression> arg;
        arg.reset($3);
        $$->addArgument(std::move(arg));
    }
    ;

type_spec
    : KW_BOOL {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::BOOL);
    }
    | KW_INT8 {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::INT8);
    }
    | KW_INT16 {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::INT16);
    }
    | KW_INT32 {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::INT32);
    }
    | KW_INT64 {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::INT64);
    }
    | KW_INT {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::INT64);
    }
    | KW_FLOAT {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::FLOAT);
    }
    | KW_DOUBLE {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::DOUBLE);
    }
    | KW_STRING {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::STRING);
    }
    | KW_FIXED_STRING L_PAREN INTEGER R_PAREN {
        if ($3 > std::numeric_limits<int16_t>::max()) {
            throw nebula::GraphParser::syntax_error(@3, "Out of range:");
        }
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::FIXED_STRING);
        $$->set_type_length($3);
    }
    | KW_TIMESTAMP {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::TIMESTAMP);
    }
    | KW_DATE {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::DATE);
    }
    | KW_TIME {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::TIME);
    }
    | KW_DATETIME {
        $$ = new meta::cpp2::ColumnTypeDef();
        $$->set_type(meta::cpp2::PropertyType::DATETIME);
    }
    ;


container_expression
    : list_expression {
        $$ = $1;
    }
    | set_expression {
        $$ = $1;
    }
    | map_expression {
        $$ = $1;
    }
    ;

list_expression
    : L_BRACKET expression_list R_BRACKET {
        $$ = new ListExpression($2);
    }
    ;

set_expression
    : L_BRACE expression_list R_BRACE {
        $$ = new SetExpression($2);
    }
    ;

expression_list
    : expression {
        $$ = new ExpressionList();
        $$->add($1);
    }
    | expression_list COMMA expression {
        $$ = $1;
        $$->add($3);
    }
    ;

map_expression
    : L_BRACE map_item_list R_BRACE {
        $$ = new MapExpression($2);
    }
    ;

map_item_list
    : name_label COLON expression {
        $$ = new MapItemList();
        $$->add($1, $3);
    }
    | map_item_list COMMA name_label COLON expression {
        $$ = $1;
        $$->add($3, $5);
    }
    ;

go_sentence
    : KW_GO step_clause from_clause over_clause where_clause yield_clause {
        auto go = new GoSentence();
        go->setStepClause($2);
        go->setFromClause($3);
        go->setOverClause($4);
        go->setWhereClause($5);
        if ($6 == nullptr) {
            auto *cols = new YieldColumns();
            if (!$4->isOverAll()) {
                for (auto e : $4->edges()) {
                    auto *edge  = new std::string(*e->edge());
                    auto *expr  = new EdgeDstIdExpression(edge);
                    auto *col   = new YieldColumn(expr);
                    cols->addColumn(col);
                }
            }
            $6 = new YieldClause(cols);
        }
        go->setYieldClause($6);
        $$ = go;
    }
    ;

step_clause
    : %empty { $$ = new StepClause(); }
    | legal_integer KW_STEPS {
        $$ = new StepClause($1);
    }
    | legal_integer KW_TO legal_integer KW_STEPS {
        $$ = new StepClause($1, $3);
    }
    ;

from_clause
    : KW_FROM vid_list {
        $$ = new FromClause($2);
    }
    | KW_FROM vid_ref_expression {
        $$ = new FromClause($2);
    }
    ;

vid_list
    : vid {
        $$ = new VertexIDList();
        $$->add($1);
    }
    | vid_list COMMA vid {
        $$ = $1;
        $$->add($3);
    }
    ;

vid
    : unary_integer {
        $$ = new ConstantExpression($1);
    }
    | function_call_expression {
        $$ = $1;
    }
    | uuid_expression {
        $$ = $1;
    }
    | STRING {
        $$ = new ConstantExpression(*$1);
        delete $1;
    }
    ;

unary_integer
    : PLUS legal_integer {
        $$ = $2;
    }
    | MINUS INTEGER {
        $$ = -$2;
    }
    | legal_integer {
        $$ = $1;
    }
    ;

vid_ref_expression
    : input_prop_expression {
        $$ = $1;
    }
    | var_prop_expression {
        $$ = $1;
    }
    ;

over_edge
    : name_label {
        $$ = new OverEdge($1);
    }
    | name_label KW_AS name_label {
        $$ = new OverEdge($1, $3);
    }
    ;

over_edges
    : over_edge {
        auto edge = new OverEdges();
        edge->addEdge($1);
        $$ = edge;
    }
    | over_edges COMMA over_edge {
        $1->addEdge($3);
        $$ = $1;
    }
    ;

over_clause
    : KW_OVER STAR {
        $$ = new OverClause(true);
    }
    | KW_OVER STAR KW_REVERSELY {
        $$ = new OverClause(true, storage::cpp2::EdgeDirection::IN_EDGE);
    }
    | KW_OVER STAR KW_BIDIRECT {
        $$ = new OverClause(true, storage::cpp2::EdgeDirection::BOTH);
    }
    | KW_OVER over_edges {
        $$ = new OverClause($2);
    }
    | KW_OVER over_edges KW_REVERSELY {
        $$ = new OverClause($2, storage::cpp2::EdgeDirection::IN_EDGE);
    }
    | KW_OVER over_edges KW_BIDIRECT {
        $$ = new OverClause($2, storage::cpp2::EdgeDirection::BOTH);
    }
    ;

where_clause
    : %empty { $$ = nullptr; }
    | KW_WHERE expression { $$ = new WhereClause($2); }
    ;

when_clause
    : %empty { $$ = nullptr; }
    | KW_WHEN expression { $$ = new WhenClause($2); }
    ;

yield_clause
    : %empty { $$ = nullptr; }
    | KW_YIELD yield_columns { $$ = new YieldClause($2); }
    | KW_YIELD KW_DISTINCT yield_columns { $$ = new YieldClause($3, true); }
    ;

yield_columns
    : yield_column {
        auto fields = new YieldColumns();
        fields->addColumn($1);
        $$ = fields;
    }
    | yield_columns COMMA yield_column {
        $1->addColumn($3);
        $$ = $1;
    }
    ;

yield_column
    : expression {
        $$ = new YieldColumn($1);
    }
    | expression KW_AS name_label {
        $$ = new YieldColumn($1, $3);
    }
    ;

group_clause
    : yield_columns { $$ = new GroupClause($1); }
    ;

yield_sentence
    : KW_YIELD yield_columns where_clause {
        auto *s = new YieldSentence($2);
        s->setWhereClause($3);
        $$ = s;
    }
    | KW_YIELD KW_DISTINCT yield_columns where_clause {
        auto *s = new YieldSentence($3, true);
        s->setWhereClause($4);
        $$ = s;
    }
    | KW_RETURN yield_columns {
        auto *s = new YieldSentence($2);
        $$ = s;
    }
    | KW_RETURN KW_DISTINCT yield_columns {
        auto *s = new YieldSentence($3, true);
        $$ = s;
    }
    ;

unwind_clause
    : KW_UNWIND expression KW_AS name_label {
        $$ = new UnwindClause($2, $4);
    }
    ;

with_clause
    : KW_WITH yield_columns match_order_by match_skip match_limit where_clause {
        $$ = new WithClause($2, $3, $4, $5, $6, false/*distinct*/);
    }
    | KW_WITH KW_DISTINCT yield_columns match_order_by match_skip match_limit where_clause {
        $$ = new WithClause($3, $4, $5, $6, $7, true);
    }
    ;

match_clause
    : KW_MATCH match_path where_clause {
        $$ = new MatchClause($2, $3, false/*optinal*/);
    }
    | KW_OPTIONAL KW_MATCH match_path where_clause {
        $$ = new MatchClause($3, $4, true);
    }
    ;

reading_clause
    : unwind_clause {
        $$ = $1;
    }
    | match_clause {
        $$ = $1;
    }
    ;

reading_clauses
    : reading_clause {
        $$ = new MatchClauseList();
        $$->add($1);
    }
    | reading_clauses reading_clause {
        $$ = $1;
        $$->add($2);
    }
    ;

reading_with_clause
    : with_clause {
        $$ = new MatchClauseList();
        $$->add($1);
    }
    | reading_clauses with_clause {
        $$ = $1;
        $$->add($2);
    }
    ;

reading_with_clauses
    : reading_with_clause {
        $$ = $1;
    }
    | reading_with_clauses reading_with_clause {
        $$ = $1;
        $$->add($2);
    }
    ;

match_sentence
    : reading_clauses match_return {
        $$ = new MatchSentence($1, $2);
    }
    | reading_with_clauses match_return {
        $$ = new MatchSentence($1, $2);
    }
    | reading_with_clauses reading_clauses match_return {
        $1->add($2);
        $$ = new MatchSentence($1, $3);
    }
    ;

match_path_pattern
    : match_node {
        $$ = new MatchPath($1);
    }
    | match_path_pattern match_edge match_node {
        $$ = $1;
        $$->add($2, $3);
    }
    ;

match_path
    : match_path_pattern {
        $$ = $1;
    }
    | name_label ASSIGN match_path_pattern {
        $$ = $3;
        $$->setAlias($1);
    }
    ;

match_node
    : L_PAREN match_alias R_PAREN {
        $$ = new MatchNode($2, nullptr, nullptr);
    }
    | L_PAREN match_alias match_node_label_list R_PAREN {
        $$ = new MatchNode($2, $3, nullptr);
    }
    | L_PAREN match_alias map_expression R_PAREN {
        $$ = new MatchNode($2, nullptr, $3);
    }
    ;

match_node_label
    : COLON name_label {
        $$ = new MatchNodeLabel($2);
    }
    | COLON name_label map_expression {
        $$ = new MatchNodeLabel($2, $3);
    }
    ;

match_node_label_list
    : match_node_label {
        $$ = new MatchNodeLabelList();
        $$->add($1);
    }
    | match_node_label_list match_node_label {
        $$ = $1;
        $1->add($2);
    }
    ;

match_alias
    : %empty {
        $$ = nullptr;
    }
    | name_label {
        $$ = $1;
    }
    ;

match_edge
    : MINUS match_edge_prop MINUS {
        $$ = new MatchEdge($2, storage::cpp2::EdgeDirection::BOTH);
    }
    | MINUS match_edge_prop R_ARROW {
        $$ = new MatchEdge($2, storage::cpp2::EdgeDirection::OUT_EDGE);
    }
    | L_ARROW match_edge_prop MINUS {
        $$ = new MatchEdge($2, storage::cpp2::EdgeDirection::IN_EDGE);
    }
    | L_ARROW match_edge_prop R_ARROW {
        $$ = new MatchEdge($2, storage::cpp2::EdgeDirection::BOTH);
    }
    ;

match_edge_prop
    : %empty {
        $$ = nullptr;
    }
    | L_BRACKET match_alias opt_match_edge_type_list match_step_range R_BRACKET {
        $$ = new MatchEdgeProp($2, $3, $4, nullptr);
    }
    | L_BRACKET match_alias opt_match_edge_type_list match_step_range map_expression R_BRACKET {
        $$ = new MatchEdgeProp($2, $3, $4, $5);
    }
    ;

opt_match_edge_type_list
    : %empty {
        $$ = nullptr;
    }
    | match_edge_type_list {
        $$ = $1;
    }
    ;

match_step_range
    : %empty {
        $$ = nullptr;
    }
    | STAR {
        $$ = new MatchStepRange(1);
    }
    | STAR legal_integer {
        $$ = new MatchStepRange($2, $2);
    }
    | STAR DOT_DOT legal_integer {
        $$ = new MatchStepRange(1, $3);
    }
    | STAR legal_integer DOT_DOT {
        $$ = new MatchStepRange($2);
    }
    | STAR legal_integer DOT_DOT legal_integer {
        $$ = new MatchStepRange($2, $4);
    }
    ;

match_edge_type_list
    : COLON name_label {
        $$ = new MatchEdgeTypeList();
        $$->add($2);
    }
    | match_edge_type_list PIPE name_label {
        $$ = $1;
        $$->add($3);
    }
    | match_edge_type_list PIPE COLON name_label {
        $$ = $1;
        $$->add($4);
    }
    ;

match_return
    : KW_RETURN yield_columns match_order_by match_skip match_limit {
        $$ = new MatchReturn($2, $3, $4, $5);
    }
    | KW_RETURN KW_DISTINCT yield_columns match_order_by match_skip match_limit {
        $$ = new MatchReturn($3, $4, $5, $6, true);
    }
    | KW_RETURN STAR match_order_by match_skip match_limit {
        $$ = new MatchReturn(nullptr, $3, $4, $5);
    }
    | KW_RETURN KW_DISTINCT STAR match_order_by match_skip match_limit {
        $$ = new MatchReturn(nullptr, $4, $5, $6, true);
    }
    ;

match_order_by
    : %empty {
        $$ = nullptr;
    }
    | KW_ORDER KW_BY order_factors {
        $$ = $3;
    }
    ;

match_skip
    : %empty {
        $$ = nullptr;
    }
    | KW_SKIP expression {
        $$ = $2;
    }
    ;

match_limit
    : %empty {
        $$ = nullptr;
    }
    | KW_LIMIT expression {
        $$ = $2;
    }
    ;


text_search_client_item
    : L_PAREN host_item R_PAREN {
        $$ = new nebula::meta::cpp2::FTClient();
        $$->set_host(*$2);
        delete $2;
    }
    | L_PAREN host_item COMMA STRING COMMA STRING R_PAREN {
        $$ = new nebula::meta::cpp2::FTClient();
        $$->set_host(*$2);
        $$->set_user(*$4);
        $$->set_pwd(*$6);
        delete $2;
        delete $4;
        delete $6;
    }
    ;

text_search_client_list
    : text_search_client_item {
        $$ = new TSClientList();
        $$->addClient($1);
    }
    | text_search_client_list COMMA text_search_client_item {
        $$ = $1;
        $$->addClient($3);
    }
    | text_search_client_list COMMA {
        $$ = $1;
    }
    ;

sign_in_text_search_service_sentence
    : KW_SIGN KW_IN KW_TEXT KW_SERVICE text_search_client_list {
        $$ = new SignInTextServiceSentence($5);
    }
    ;

sign_out_text_search_service_sentence
    : KW_SIGN KW_OUT KW_TEXT KW_SERVICE {
        $$ = new SignOutTextServiceSentence();
    }
    ;

base_text_search_argument
    : name_label DOT name_label COMMA STRING {
        auto arg = new TextSearchArgument($1, $3, $5);
        $$ = arg;
    }
    ;

fuzzy_text_search_argument
   : base_text_search_argument COMMA KW_AUTO COMMA KW_AND {
        $$ = $1;
        $$->setFuzziness(-1);
        $$->setOP(new std::string("and"));
   }
   | base_text_search_argument COMMA KW_AUTO COMMA KW_OR {
        $$ = $1;
        $$->setFuzziness(-1);
        $$->setOP(new std::string("or"));
   }
   | base_text_search_argument COMMA legal_integer COMMA KW_AND {
        if ($3 != 0 && $3 != 1 && $3 != 2) {
            delete $1;
            throw nebula::GraphParser::syntax_error(@3, "Out of range:");
        }
        $$ = $1;
        $$->setFuzziness($3);
        $$->setOP(new std::string("and"));
   }
   | base_text_search_argument COMMA legal_integer COMMA KW_OR {
        if ($3 != 0 && $3 != 1 && $3 != 2) {
            delete $1;
            throw nebula::GraphParser::syntax_error(@3, "Out of range:");
        }
        $$ = $1;
        $$->setFuzziness($3);
        $$->setOP(new std::string("or"));
   }

text_search_argument
    : base_text_search_argument {
        $$ = $1;
    }
    | fuzzy_text_search_argument {
        $$ = $1;
    }
    | base_text_search_argument COMMA legal_integer {
        if ($3 < 1) {
            delete $1;
            throw nebula::GraphParser::syntax_error(@3, "Out of range:");
        }
        $$ = $1;
        $$->setLimit($3);
    }
    | base_text_search_argument COMMA legal_integer COMMA legal_integer {
        if ($3 < 1) {
            delete $1;
            throw nebula::GraphParser::syntax_error(@3, "Out of range:");
        }
        if ($5 < 1) {
            delete $1;
            throw nebula::GraphParser::syntax_error(@5, "Out of range:");
        }
        $$ = $1;
        $$->setLimit($3);
        $$->setTimeout($5);
    }
    | fuzzy_text_search_argument COMMA legal_integer {
        if ($3 < 1) {
            delete $1;
            throw nebula::GraphParser::syntax_error(@3, "Out of range:");
        }
        $$ = $1;
        $$->setLimit($3);
    }
    | fuzzy_text_search_argument COMMA legal_integer COMMA legal_integer {
        if ($3 < 1) {
            delete $1;
            throw nebula::GraphParser::syntax_error(@3, "Out of range:");
        }
        if ($5 < 1) {
            delete $1;
            throw nebula::GraphParser::syntax_error(@5, "Out of range:");
        }
        $$ = $1;
        $$->setLimit($3);
        $$->setTimeout($5);
    }
    ;

text_search_expression
    : KW_PREFIX L_PAREN text_search_argument R_PAREN {
        if ($3->op() != nullptr) {
            delete $3;
            throw nebula::GraphParser::syntax_error(@3, "argument error:");
        }
        if ($3->fuzziness() != -2) {
            delete $3;
            throw nebula::GraphParser::syntax_error(@3, "argument error:");
        }
        $$ = new TextSearchExpression(Expression::Kind::kTSPrefix, $3);
    }
    | KW_WILDCARD L_PAREN text_search_argument R_PAREN {
        if ($3->op() != nullptr) {
            delete $3;
            throw nebula::GraphParser::syntax_error(@3, "argument error:");
        }
        if ($3->fuzziness() != -2) {
            delete $3;
            throw nebula::GraphParser::syntax_error(@3, "argument error:");
        }
        $$ = new TextSearchExpression(Expression::Kind::kTSWildcard, $3);
    }
    | KW_REGEXP L_PAREN text_search_argument R_PAREN {
        if ($3->op() != nullptr) {
            delete $3;
            throw nebula::GraphParser::syntax_error(@3, "argument error:");
        }
        if ($3->fuzziness() != -2) {
            delete $3;
            throw nebula::GraphParser::syntax_error(@3, "argument error:");
        }
        $$ = new TextSearchExpression(Expression::Kind::kTSRegexp, $3);
    }
    | KW_FUZZY L_PAREN text_search_argument R_PAREN {
        $$ = new TextSearchExpression(Expression::Kind::kTSFuzzy, $3);
    }
    ;

    // TODO : unfiy the text_search_expression into expression in the future
    // The current version only support independent text_search_expression for lookup_sentence
lookup_where_clause
    : %empty { $$ = nullptr; }
    | KW_WHERE text_search_expression { $$ = new WhereClause($2); }
    | KW_WHERE expression { $$ = new WhereClause($2); }
    ;

lookup_sentence
    : KW_LOOKUP KW_ON name_label lookup_where_clause yield_clause {
        auto sentence = new LookupSentence($3);
        sentence->setWhereClause($4);
        sentence->setYieldClause($5);
        $$ = sentence;
    }
    ;

order_factor
    : expression {
        $$ = new OrderFactor($1, OrderFactor::ASCEND);
    }
    | expression KW_ASC {
        $$ = new OrderFactor($1, OrderFactor::ASCEND);
    }
    | expression KW_DESC {
        $$ = new OrderFactor($1, OrderFactor::DESCEND);
    }
    | expression KW_ASCENDING {
        $$ = new OrderFactor($1, OrderFactor::ASCEND);
    }
    | expression KW_DESCENDING {
        $$ = new OrderFactor($1, OrderFactor::DESCEND);
    }
    ;

order_factors
    : order_factor {
        auto factors = new OrderFactors();
        factors->addFactor($1);
        $$ = factors;
    }
    | order_factors COMMA order_factor {
        $1->addFactor($3);
        $$ = $1;
    }
    ;

order_by_sentence
    : KW_ORDER KW_BY order_factors {
        $$ = new OrderBySentence($3);
    }
    ;

fetch_vertices_sentence
    : KW_FETCH KW_PROP KW_ON name_label_list vid_list yield_clause {
        $$ = new FetchVerticesSentence($4, $5, $6);
    }
    | KW_FETCH KW_PROP KW_ON name_label_list vid_ref_expression yield_clause {
        $$ = new FetchVerticesSentence($4, $5, $6);
    }
    | KW_FETCH KW_PROP KW_ON STAR vid_list yield_clause {
        $$ = new FetchVerticesSentence($5, $6);
    }
    | KW_FETCH KW_PROP KW_ON STAR vid_ref_expression yield_clause {
        $$ = new FetchVerticesSentence($5, $6);
    }
    ;

edge_key
    : vid R_ARROW vid AT rank {
        $$ = new EdgeKey($1, $3, $5);
    }
    | vid R_ARROW vid {
        $$ = new EdgeKey($1, $3, 0);
    }
    ;

edge_keys
    : edge_key {
        auto edgeKeys = new EdgeKeys();
        edgeKeys->addEdgeKey($1);
        $$ = edgeKeys;
    }
    | edge_keys COMMA edge_key {
        $1->addEdgeKey($3);
        $$ = $1;
    }
    ;

edge_key_ref:
    input_prop_expression R_ARROW input_prop_expression AT input_prop_expression {
        $$ = new EdgeKeyRef($1, $3, $5);
    }
    |
    var_prop_expression R_ARROW var_prop_expression AT var_prop_expression {
        $$ = new EdgeKeyRef($1, $3, $5, false);
    }
    |
    input_prop_expression R_ARROW input_prop_expression {
        $$ = new EdgeKeyRef($1, $3, new ConstantExpression(0));
    }
    |
    var_prop_expression R_ARROW var_prop_expression {
        $$ = new EdgeKeyRef($1, $3, new ConstantExpression(0), false);
    }
    ;

fetch_edges_sentence
    : KW_FETCH KW_PROP KW_ON name_label_list edge_keys yield_clause {
        auto fetch = new FetchEdgesSentence($4, $5, $6);
        $$ = fetch;
    }
    | KW_FETCH KW_PROP KW_ON name_label_list edge_key_ref yield_clause {
        auto fetch = new FetchEdgesSentence($4, $5, $6);
        $$ = fetch;
    }
    ;

fetch_sentence
    : fetch_vertices_sentence { $$ = $1; }
    | fetch_edges_sentence { $$ = $1; }
    ;

find_path_sentence
    : KW_FIND KW_ALL KW_PATH opt_with_properites from_clause to_clause over_clause find_path_upto_clause
    /* where_clause */ {
        auto *s = new FindPathSentence(false, $4, false);
        s->setFrom($5);
        s->setTo($6);
        s->setOver($7);
        s->setStep($8);
        /* s->setWhere($9); */
        $$ = s;
    }
    | KW_FIND KW_SHORTEST KW_PATH opt_with_properites from_clause to_clause over_clause find_path_upto_clause
    /* where_clause */ {
        auto *s = new FindPathSentence(true, $4, false);
        s->setFrom($5);
        s->setTo($6);
        s->setOver($7);
        s->setStep($8);
        /* s->setWhere($9); */
        $$ = s;
    }
    | KW_FIND KW_NOLOOP KW_PATH opt_with_properites from_clause to_clause over_clause find_path_upto_clause
    /* where_clause */ {
        auto *s = new FindPathSentence(false, $4, true);
        s->setFrom($5);
        s->setTo($6);
        s->setOver($7);
        s->setStep($8);
        /* s->setWhere($9) */
        $$ = s;
    }
    ;

opt_with_properites
    : %empty { $$ = false; }
    | KW_WITH KW_PROP { $$ = true; }
    ;

find_path_upto_clause
    : %empty { $$ = new StepClause(5); }
    | KW_UPTO legal_integer KW_STEPS {
        $$ = new StepClause($2);
    }
    ;

to_clause
    : KW_TO vid_list {
        $$ = new ToClause($2);
    }
    | KW_TO vid_ref_expression {
        $$ = new ToClause($2);
    }
    ;

limit_sentence
    : KW_LIMIT legal_integer {
        $$ = new LimitSentence(0, $2);
    }
    | KW_LIMIT legal_integer COMMA legal_integer {
        $$ = new LimitSentence($2, $4);
    }
    | KW_LIMIT legal_integer KW_OFFSET legal_integer {
        $$ = new LimitSentence($4, $2);
    }
    | KW_OFFSET legal_integer KW_LIMIT legal_integer  {
        $$ = new LimitSentence($2, $4);
    }
    ;

group_by_sentence
    : KW_GROUP KW_BY group_clause yield_clause {
        auto group = new GroupBySentence();
        group->setGroupClause($3);
        group->setYieldClause($4);
        $$ = group;
    }
    ;

in_bound_clause
    : %empty { $$ = nullptr; }
    | KW_IN over_edges { $$ = new InBoundClause($2, BoundClause::IN); }

out_bound_clause
    : %empty { $$ = nullptr; }
    | KW_OUT over_edges { $$ = new OutBoundClause($2, BoundClause::OUT); }

both_in_out_clause
    : %empty { $$ = nullptr; }
    | KW_BOTH over_edges { $$ = new BothInOutClause($2, BoundClause::BOTH); }

get_subgraph_sentence
    : KW_GET KW_SUBGRAPH step_clause from_clause in_bound_clause out_bound_clause both_in_out_clause {
        $$ = new GetSubgraphSentence($3, $4, $5, $6, $7);
    }

use_sentence
    : KW_USE name_label { $$ = new UseSentence($2); }
    ;

opt_if_not_exists
    : %empty { $$=false; }
    | KW_IF KW_NOT KW_EXISTS { $$=true; }
    ;

opt_if_exists
    : %empty { $$=false; }
    | KW_IF KW_EXISTS { $$=true; }
    ;

opt_create_schema_prop_list
    : %empty {
        $$ = nullptr;
    }
    | create_schema_prop_list {
        $$ = $1;
    }
    ;

create_schema_prop_list
    : create_schema_prop_item {
        $$ = new SchemaPropList();
        $$->addOpt($1);
    }
    | create_schema_prop_list COMMA create_schema_prop_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

create_schema_prop_item
    : KW_TTL_DURATION ASSIGN legal_integer {
        $$ = new SchemaPropItem(SchemaPropItem::TTL_DURATION, $3);
    }
    | KW_TTL_COL ASSIGN STRING {
        $$ = new SchemaPropItem(SchemaPropItem::TTL_COL, *$3);
        delete $3;
    }
    ;

create_tag_sentence
    : KW_CREATE KW_TAG opt_if_not_exists name_label L_PAREN R_PAREN opt_create_schema_prop_list {
        if ($7 == nullptr) {
            $7 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($4, new ColumnSpecificationList(), $7, $3);
    }
    | KW_CREATE KW_TAG opt_if_not_exists name_label L_PAREN column_spec_list R_PAREN opt_create_schema_prop_list {
        if ($8 == nullptr) {
            $8 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($4, $6, $8, $3);
    }
    | KW_CREATE KW_TAG opt_if_not_exists name_label L_PAREN column_spec_list COMMA R_PAREN opt_create_schema_prop_list {
        if ($9 == nullptr) {
            $9 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($4, $6, $9, $3);
    }
    ;

alter_tag_sentence
    : KW_ALTER KW_TAG name_label alter_schema_opt_list {
        $$ = new AlterTagSentence($3, $4, new SchemaPropList());
    }
    | KW_ALTER KW_TAG name_label alter_schema_prop_list {
        $$ = new AlterTagSentence($3, new AlterSchemaOptList(), $4);
    }
    | KW_ALTER KW_TAG name_label alter_schema_opt_list alter_schema_prop_list {
        $$ = new AlterTagSentence($3, $4, $5);
    }
    ;

alter_schema_opt_list
    : alter_schema_opt_item {
        $$ = new AlterSchemaOptList();
        $$->addOpt($1);
    }
    | alter_schema_opt_list COMMA alter_schema_opt_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

alter_schema_opt_item
    : KW_ADD L_PAREN column_spec_list R_PAREN {
        $$ = new AlterSchemaOptItem(AlterSchemaOptItem::ADD, $3);
    }
    | KW_CHANGE L_PAREN column_spec_list R_PAREN {
        $$ = new AlterSchemaOptItem(AlterSchemaOptItem::CHANGE, $3);
    }
    | KW_DROP L_PAREN column_name_list R_PAREN {
        $$ = new AlterSchemaOptItem(AlterSchemaOptItem::DROP, $3);
    }
    ;

alter_schema_prop_list
    : alter_schema_prop_item {
        $$ = new SchemaPropList();
        $$->addOpt($1);
    }
    | alter_schema_prop_list COMMA alter_schema_prop_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

alter_schema_prop_item
    : KW_TTL_DURATION ASSIGN legal_integer {
        $$ = new SchemaPropItem(SchemaPropItem::TTL_DURATION, $3);
    }
    | KW_TTL_COL ASSIGN STRING {
        $$ = new SchemaPropItem(SchemaPropItem::TTL_COL, *$3);
        delete $3;
    }
    ;

create_edge_sentence
    : KW_CREATE KW_EDGE opt_if_not_exists name_label L_PAREN R_PAREN opt_create_schema_prop_list {
        if ($7 == nullptr) {
            $7 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($4,  new ColumnSpecificationList(), $7, $3);
    }
    | KW_CREATE KW_EDGE opt_if_not_exists name_label L_PAREN column_spec_list R_PAREN opt_create_schema_prop_list {
        if ($8 == nullptr) {
            $8 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($4, $6, $8, $3);
    }
    | KW_CREATE KW_EDGE opt_if_not_exists name_label L_PAREN column_spec_list COMMA R_PAREN opt_create_schema_prop_list {
        if ($9 == nullptr) {
            $9 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($4, $6, $9, $3);
    }
    ;

alter_edge_sentence
    : KW_ALTER KW_EDGE name_label alter_schema_opt_list {
        $$ = new AlterEdgeSentence($3, $4, new SchemaPropList());
    }
    | KW_ALTER KW_EDGE name_label alter_schema_prop_list {
        $$ = new AlterEdgeSentence($3, new AlterSchemaOptList(), $4);
    }
    | KW_ALTER KW_EDGE name_label alter_schema_opt_list alter_schema_prop_list {
        $$ = new AlterEdgeSentence($3, $4, $5);
    }
    ;

column_name_list
    : name_label {
        $$ = new ColumnNameList();
        $$->addColumn($1);
    }
    | column_name_list COMMA name_label {
        $$ = $1;
        $$->addColumn($3);
    }
    ;

column_spec_list
    : column_spec {
        $$ = new ColumnSpecificationList();
        $$->addColumn($1);
    }
    | column_spec_list COMMA column_spec {
        $$ = $1;
        $$->addColumn($3);
    }
    ;

column_spec
    : name_label type_spec {
        $$ = new ColumnSpecification($1, $2->type, true, nullptr, $2->type_length);
        delete $2;
    }
    | name_label type_spec KW_DEFAULT expression {
        $$ = new ColumnSpecification($1, $2->type, true, $4, $2->type_length);
        delete $2;
    }
    | name_label type_spec KW_NOT KW_NULL {
        $$ = new ColumnSpecification($1, $2->type, false, nullptr, $2->type_length);
        delete $2;
    }
    | name_label type_spec KW_NULL {
        $$ = new ColumnSpecification($1, $2->type, true, nullptr, $2->type_length);
        delete $2;
    }
    | name_label type_spec KW_NOT KW_NULL KW_DEFAULT expression {
        $$ = new ColumnSpecification($1, $2->type, false, $6, $2->type_length);
        delete $2;
    }
    | name_label type_spec KW_DEFAULT expression KW_NOT KW_NULL {
        $$ = new ColumnSpecification($1, $2->type, false, $4, $2->type_length);
        delete $2;
    }
    | name_label type_spec KW_NULL KW_DEFAULT expression {
        $$ = new ColumnSpecification($1, $2->type, true, $5 , $2->type_length);
        delete $2;
    }
    | name_label type_spec KW_DEFAULT expression KW_NULL {
        $$ = new ColumnSpecification($1, $2->type, true, $4 , $2->type_length);
        delete $2;
    }
    ;

describe_tag_sentence
    : KW_DESCRIBE KW_TAG name_label {
        $$ = new DescribeTagSentence($3);
    }
    | KW_DESC KW_TAG name_label {
        $$ = new DescribeTagSentence($3);
    }
    ;

describe_edge_sentence
    : KW_DESCRIBE KW_EDGE name_label {
        $$ = new DescribeEdgeSentence($3);
    }
    | KW_DESC KW_EDGE name_label {
        $$ = new DescribeEdgeSentence($3);
    }
    ;

drop_tag_sentence
    : KW_DROP KW_TAG opt_if_exists name_label {
        $$ = new DropTagSentence($4, $3);
    }
    ;

drop_edge_sentence
    : KW_DROP KW_EDGE opt_if_exists name_label {
        $$ = new DropEdgeSentence($4, $3);
    }
    ;

index_field
    : name_label {
        $$ = new meta::cpp2::IndexFieldDef();
        $$->set_name($1->c_str());
        delete $1;
    }
    | name_label L_PAREN INTEGER R_PAREN {
        if ($3 > std::numeric_limits<int16_t>::max()) {
            throw nebula::GraphParser::syntax_error(@3, "Out of range:");
        }
        $$ = new meta::cpp2::IndexFieldDef();
        $$->set_name($1->c_str());
        $$->set_type_length($3);
        delete $1;
    }
    ;

index_field_list
    : index_field {
        $$ = new IndexFieldList();
        std::unique_ptr<meta::cpp2::IndexFieldDef> field;
        field.reset($1);
        $$->addField(std::move(field));
    }
    | index_field_list COMMA index_field {
        $$ = $1;
        std::unique_ptr<meta::cpp2::IndexFieldDef> field;
        field.reset($3);
        $$->addField(std::move(field));
    }
    ;

opt_index_field_list
    : %empty {
        $$ = nullptr;
    }
    | index_field_list {
        $$ = $1;
    }
    ;

create_tag_index_sentence
    : KW_CREATE KW_TAG KW_INDEX opt_if_not_exists name_label KW_ON name_label L_PAREN opt_index_field_list R_PAREN {
        $$ = new CreateTagIndexSentence($5, $7, $9, $4);
    }
    ;

create_edge_index_sentence
    : KW_CREATE KW_EDGE KW_INDEX opt_if_not_exists name_label KW_ON name_label L_PAREN opt_index_field_list R_PAREN {
        $$ = new CreateEdgeIndexSentence($5, $7, $9, $4);
    }
    ;

drop_tag_index_sentence
    : KW_DROP KW_TAG KW_INDEX opt_if_exists name_label {
        $$ = new DropTagIndexSentence($5, $4);
    }
    ;

drop_edge_index_sentence
    : KW_DROP KW_EDGE KW_INDEX opt_if_exists name_label {
        $$ = new DropEdgeIndexSentence($5, $4);
    }
    ;

describe_tag_index_sentence
    : KW_DESCRIBE KW_TAG KW_INDEX name_label {
        $$ = new DescribeTagIndexSentence($4);
    }
    | KW_DESC KW_TAG KW_INDEX name_label {
        $$ = new DescribeTagIndexSentence($4);
    }
    ;

describe_edge_index_sentence
    : KW_DESCRIBE KW_EDGE KW_INDEX name_label {
        $$ = new DescribeEdgeIndexSentence($4);
    }
    | KW_DESC KW_EDGE KW_INDEX name_label {
        $$ = new DescribeEdgeIndexSentence($4);
    }
    ;

rebuild_tag_index_sentence
    : KW_REBUILD KW_TAG KW_INDEX name_label_list {
        auto sentence = new AdminJobSentence(meta::cpp2::AdminJobOp::ADD,
                                             meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
        sentence->addPara(*$4);
        delete $4;
        $$ = sentence;
    }
    | KW_REBUILD KW_TAG KW_INDEX {
        $$ = new AdminJobSentence(meta::cpp2::AdminJobOp::ADD,
                                  meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    }
    ;

rebuild_edge_index_sentence
    : KW_REBUILD KW_EDGE KW_INDEX name_label_list {
        auto sentence = new AdminJobSentence(meta::cpp2::AdminJobOp::ADD,
                                             meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
        sentence->addPara(*$4);
        delete $4;
        $$ = sentence;
    }
    | KW_REBUILD KW_EDGE KW_INDEX {
        $$ = new AdminJobSentence(meta::cpp2::AdminJobOp::ADD,
                                  meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    }
    ;

add_group_sentence
    : KW_ADD KW_GROUP name_label zone_name_list{
        $$ = new AddGroupSentence($3, $4);
    }
    ;

drop_group_sentence
    : KW_DROP KW_GROUP name_label {
        $$ = new DropGroupSentence($3);
    }
    ;

desc_group_sentence
    : KW_DESCRIBE KW_GROUP name_label {
        $$ = new DescribeGroupSentence($3);
    }
    | KW_DESC KW_GROUP name_label {
        $$ = new DescribeGroupSentence($3);
    }
    ;

add_zone_into_group_sentence
    : KW_ADD KW_ZONE name_label KW_INTO KW_GROUP name_label {
        $$ = new AddZoneIntoGroupSentence($3, $6);
    }
    ;

drop_zone_from_group_sentence
    : KW_DROP KW_ZONE name_label KW_FROM KW_GROUP name_label {
        $$ = new DropZoneFromGroupSentence($3, $6);
    }
    ;

add_zone_sentence
    : KW_ADD KW_ZONE name_label host_list {
        $$ = new AddZoneSentence($3, $4);
    }
    ;

drop_zone_sentence
    : KW_DROP KW_ZONE name_label {
        $$ = new DropZoneSentence($3);
    }
    ;

desc_zone_sentence
    : KW_DESCRIBE KW_ZONE name_label {
        $$ = new DescribeZoneSentence($3);
    }
    | KW_DESC KW_ZONE name_label {
        $$ = new DescribeZoneSentence($3);
    }
    ;

add_host_into_zone_sentence
    : KW_ADD KW_HOST host_item KW_INTO KW_ZONE name_label {
        $$ = new AddHostIntoZoneSentence($3, $6);
    }
    ;

drop_host_from_zone_sentence
    : KW_DROP KW_HOST host_item KW_FROM KW_ZONE name_label {
        $$ = new DropHostFromZoneSentence($3, $6);
    }
    ;

traverse_sentence
    : L_PAREN set_sentence R_PAREN { $$ = $2; }
    | go_sentence { $$ = $1; }
    | match_sentence { $$ = $1; }
    | lookup_sentence { $$ = $1; }
    | group_by_sentence { $$ = $1; }
    | order_by_sentence { $$ = $1; }
    | fetch_sentence { $$ = $1; }
    | find_path_sentence { $$ = $1; }
    | limit_sentence { $$ = $1; }
    | yield_sentence { $$ = $1; }
    | get_subgraph_sentence { $$ = $1; }
    | delete_vertex_sentence { $$ = $1; }
    | delete_edge_sentence { $$ = $1; }
    ;

piped_sentence
    : traverse_sentence { $$ = $1; }
    | piped_sentence PIPE traverse_sentence { $$ = new PipedSentence($1, $3); }
    ;

set_sentence
    : piped_sentence { $$ = $1; }
    | set_sentence KW_UNION KW_ALL piped_sentence { $$ = new SetSentence($1, SetSentence::UNION, $4); }
    | set_sentence KW_UNION piped_sentence {
        auto *s = new SetSentence($1, SetSentence::UNION, $3);
        s->setDistinct();
        $$ = s;
    }
    | set_sentence KW_UNION KW_DISTINCT piped_sentence {
        auto *s = new SetSentence($1, SetSentence::UNION, $4);
        s->setDistinct();
        $$ = s;
    }
    | set_sentence KW_INTERSECT piped_sentence { $$ = new SetSentence($1, SetSentence::INTERSECT, $3); }
    | set_sentence KW_MINUS piped_sentence { $$ = new SetSentence($1, SetSentence::MINUS, $3); }
    ;

assignment_sentence
    : VARIABLE ASSIGN set_sentence {
        $$ = new AssignmentSentence($1, $3);
    }
    ;

insert_vertex_sentence
    : KW_INSERT KW_VERTEX vertex_tag_list KW_VALUES vertex_row_list {
        $$ = new InsertVerticesSentence($3, $5);
    }
    | KW_INSERT KW_VERTEX KW_NO KW_OVERWRITE vertex_tag_list KW_VALUES vertex_row_list {
        $$ = new InsertVerticesSentence($5, $7, false /* not overwritable */);
    }
    ;

vertex_tag_list
    : vertex_tag_item {
        $$ = new VertexTagList();
        $$->addTagItem($1);
    }
    | vertex_tag_list COMMA vertex_tag_item {
        $$ = $1;
        $$->addTagItem($3);
    }
    ;

vertex_tag_item
    : name_label L_PAREN R_PAREN{
        $$ = new VertexTagItem($1);
    }
    | name_label L_PAREN prop_list R_PAREN {
        $$ = new VertexTagItem($1, $3);
    }
    ;

prop_list
    : name_label {
        $$ = new PropertyList();
        $$->addProp($1);
    }
    | prop_list COMMA name_label {
        $$ = $1;
        $$->addProp($3);
    }
    | prop_list COMMA {
        $$ = $1;
    }
    ;

vertex_row_list
    : vertex_row_item {
        $$ = new VertexRowList();
        $$->addRow($1);
    }
    | vertex_row_list COMMA vertex_row_item {
        $1->addRow($3);
        $$ = $1;
    }
    ;

vertex_row_item
    : vid COLON L_PAREN R_PAREN {
        $$ = new VertexRowItem($1, new ValueList());
    }
    | vid COLON L_PAREN value_list R_PAREN {
        $$ = new VertexRowItem($1, $4);
    }
    ;

value_list
    : expression {
        $$ = new ValueList();
        $$->addValue($1);
    }
    | value_list COMMA expression {
        $$ = $1;
        $$->addValue($3);
    }
    | value_list COMMA {
        $$ = $1;
    }
    ;

insert_edge_sentence
    : KW_INSERT KW_EDGE name_label L_PAREN R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgesSentence();
        sentence->setEdge($3);
        sentence->setProps(new PropertyList());
        sentence->setRows($7);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE name_label L_PAREN prop_list R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgesSentence();
        sentence->setEdge($3);
        sentence->setProps($5);
        sentence->setRows($8);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE name_label L_PAREN R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgesSentence();
        sentence->setOverwrite(false);
        sentence->setEdge($5);
        sentence->setProps(new PropertyList());
        sentence->setRows($9);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE name_label L_PAREN prop_list R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgesSentence();
        sentence->setOverwrite(false);
        sentence->setEdge($5);
        sentence->setProps($7);
        sentence->setRows($10);
        $$ = sentence;
    }
    ;

edge_row_list
    : edge_row_item {
        $$ = new EdgeRowList();
        $$->addRow($1);
    }
    | edge_row_list COMMA edge_row_item {
        $1->addRow($3);
        $$ = $1;
    }
    ;

edge_row_item
    : vid R_ARROW vid COLON L_PAREN R_PAREN {
        $$ = new EdgeRowItem($1, $3, new ValueList());
    }
    | vid R_ARROW vid COLON L_PAREN value_list R_PAREN {
        $$ = new EdgeRowItem($1, $3, $6);
    }
    | vid R_ARROW vid AT rank COLON L_PAREN R_PAREN {
        $$ = new EdgeRowItem($1, $3, $5, new ValueList());
    }
    | vid R_ARROW vid AT rank COLON L_PAREN value_list R_PAREN {
        $$ = new EdgeRowItem($1, $3, $5, $8);
    }
    ;

rank: unary_integer { $$ = $1; };

update_list
    : update_item {
        $$ = new UpdateList();
        $$->addItem($1);
    }
    | update_list COMMA update_item {
        $$ = $1;
        $$->addItem($3);
    }
    ;

update_item
    : name_label ASSIGN expression {
        $$ = new UpdateItem($1, $3);
    }
    | name_label DOT name_label ASSIGN expression {
        auto expr = new LabelAttributeExpression(new LabelExpression($1),
                                                 new ConstantExpression(*$3));
        $$ = new UpdateItem(expr, $5);
        delete $3;
    }
    ;

update_vertex_sentence
    // ======== Begin: Compatible with 1.0 =========
    : KW_UPDATE KW_VERTEX vid KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateVertexSentence($3, $5, $6, $7);
        $$ = sentence;
    }
    | KW_UPSERT KW_VERTEX vid KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateVertexSentence($3, $5, $6, $7, true);
        $$ = sentence;
    }
     // ======== End: Compatible with 1.0 =========
    | KW_UPDATE KW_VERTEX KW_ON name_label vid KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateVertexSentence($5, $4, $7, $8, $9);
        $$ = sentence;
    }
    | KW_UPSERT KW_VERTEX KW_ON name_label vid KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateVertexSentence($5, $4, $7, $8, $9, true);
        $$ = sentence;
    }
    ;

update_edge_sentence
    // ======== Begin: Compatible with 1.0 =========
    : KW_UPDATE KW_EDGE vid R_ARROW vid KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence($3, $5, 0, $7, $9, $10, $11);
        $$ = sentence;
    }
    | KW_UPSERT KW_EDGE vid R_ARROW vid KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence($3, $5, 0, $7, $9, $10, $11, true);
        $$ = sentence;
    }
    | KW_UPDATE KW_EDGE vid R_ARROW vid AT rank KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence($3, $5, $7, $9, $11, $12, $13);
        $$ = sentence;
    }
    | KW_UPSERT KW_EDGE vid R_ARROW vid AT rank KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence($3, $5, $7, $9, $11, $12, $13, true);
        $$ = sentence;
    }
    // ======== End: Compatible with 1.0 =========
    | KW_UPDATE KW_EDGE KW_ON name_label vid R_ARROW vid
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence($5, $7, 0, $4, $9, $10, $11);
        $$ = sentence;
    }
    | KW_UPSERT KW_EDGE KW_ON name_label vid R_ARROW vid
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence($5, $7, 0, $4, $9, $10, $11, true);
        $$ = sentence;
    }
    | KW_UPDATE KW_EDGE KW_ON name_label vid R_ARROW vid AT rank
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence($5, $7, $9, $4, $11, $12, $13);
        $$ = sentence;
    }
    | KW_UPSERT KW_EDGE KW_ON name_label vid R_ARROW vid AT rank
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence($5, $7, $9, $4, $11, $12, $13, true);
        $$ = sentence;
    }
    ;

delete_vertex_sentence
    : KW_DELETE KW_VERTEX vid_list {
        auto sentence = new DeleteVerticesSentence($3);
        $$ = sentence;
    }
    | KW_DELETE KW_VERTEX vid_ref_expression {
        auto sentence = new DeleteVerticesSentence($3);
        $$ = sentence;
    }
    ;

download_sentence
    : KW_DOWNLOAD KW_HDFS STRING {
        auto sentence = new DownloadSentence();
        sentence->setUrl(*$3);
        $$ = sentence;
        delete $3;
    }
    ;

delete_edge_sentence
    : KW_DELETE KW_EDGE name_label edge_keys {
        auto sentence = new DeleteEdgesSentence($3, $4);
        $$ = sentence;
    }

    | KW_DELETE KW_EDGE name_label edge_key_ref {
        auto sentence = new DeleteEdgesSentence($3, $4);
        $$ = sentence;
    }
    ;

ingest_sentence
    : KW_INGEST {
        auto sentence = new IngestSentence();
        $$ = sentence;
    }
    ;

admin_job_sentence
    : KW_SUBMIT KW_JOB KW_COMPACT job_concurrency {
        auto sentence = new AdminJobSentence(meta::cpp2::AdminJobOp::ADD,
                                             meta::cpp2::AdminCmd::COMPACT);
        if ($4 != 0) {
            sentence->addPara(std::to_string($4));
        }
        $$ = sentence;
    }
    | KW_SUBMIT KW_JOB KW_FLUSH job_concurrency {
        auto sentence = new AdminJobSentence(meta::cpp2::AdminJobOp::ADD,
                                             meta::cpp2::AdminCmd::FLUSH);
        if ($4 != 0) {
            sentence->addPara(std::to_string($4));
        }
        $$ = sentence;
    }
    | KW_SUBMIT KW_JOB KW_STATS job_concurrency {
        auto sentence = new AdminJobSentence(meta::cpp2::AdminJobOp::ADD,
                                             meta::cpp2::AdminCmd::STATS);
        if ($4 != 0) {
            sentence->addPara(std::to_string($4));
        }
        $$ = sentence;
    }
    | KW_SHOW KW_JOBS {
        auto sentence = new AdminJobSentence(meta::cpp2::AdminJobOp::SHOW_All);
        $$ = sentence;
    }
    | KW_SHOW KW_JOB legal_integer {
        auto sentence = new AdminJobSentence(meta::cpp2::AdminJobOp::SHOW);
        sentence->addPara(std::to_string($3));
        $$ = sentence;
    }
    | KW_STOP KW_JOB legal_integer {
        auto sentence = new AdminJobSentence(meta::cpp2::AdminJobOp::STOP);
        sentence->addPara(std::to_string($3));
        $$ = sentence;
    }
    | KW_RECOVER KW_JOB {
        auto sentence = new AdminJobSentence(meta::cpp2::AdminJobOp::RECOVER);
        $$ = sentence;
    }
    ;

job_concurrency
    : %empty {
        $$ = 0;
    }
    | legal_integer {
        $$ = $1;
    }
    ;

show_sentence
    : KW_SHOW KW_HOSTS {
        $$ = new ShowHostsSentence(meta::cpp2::ListHostType::ALLOC);
    }
    | KW_SHOW KW_HOSTS list_host_type {
        $$ = new ShowHostsSentence($3);
    }
    | KW_SHOW KW_SPACES {
        $$ = new ShowSpacesSentence();
    }
    | KW_SHOW KW_PARTS {
        $$ = new ShowPartsSentence();
    }
    | KW_SHOW KW_PART integer_list {
        $$ = new ShowPartsSentence($3);
    }
    | KW_SHOW KW_PARTS integer_list {
        $$ = new ShowPartsSentence($3);
    }
    | KW_SHOW KW_TAGS {
        $$ = new ShowTagsSentence();
    }
    | KW_SHOW KW_EDGES {
         $$ = new ShowEdgesSentence();
    }
    | KW_SHOW KW_TAG KW_INDEXES {
         $$ = new ShowTagIndexesSentence();
    }
    | KW_SHOW KW_EDGE KW_INDEXES {
         $$ = new ShowEdgeIndexesSentence();
    }
    | KW_SHOW KW_USERS {
        $$ = new ShowUsersSentence();
    }
    | KW_SHOW KW_ROLES KW_IN name_label {
        $$ = new ShowRolesSentence($4);
    }
    | KW_SHOW KW_CONFIGS show_config_item {
        $$ = new ShowConfigsSentence($3);
    }
    | KW_SHOW KW_CREATE KW_SPACE name_label {
        $$ = new ShowCreateSpaceSentence($4);
    }
    | KW_SHOW KW_CREATE KW_TAG name_label {
        $$ = new ShowCreateTagSentence($4);
    }
    | KW_SHOW KW_CREATE KW_TAG KW_INDEX name_label {
        $$ = new ShowCreateTagIndexSentence($5);
    }
    | KW_SHOW KW_CREATE KW_EDGE name_label {
        $$ = new ShowCreateEdgeSentence($4);
    }
    | KW_SHOW KW_CREATE KW_EDGE KW_INDEX name_label {
        $$ = new ShowCreateEdgeIndexSentence($5);
    }
    | KW_SHOW KW_TAG KW_INDEX KW_STATUS {
        auto sentence = new ShowTagIndexStatusSentence();
        $$ = sentence;
    }
    | KW_SHOW KW_EDGE KW_INDEX KW_STATUS  {
        auto sentence = new ShowEdgeIndexStatusSentence();
        $$ = sentence;
    }
    | KW_SHOW KW_SNAPSHOTS {
        $$ = new ShowSnapshotsSentence();
    }
    | KW_SHOW KW_CHARSET {
        $$ = new ShowCharsetSentence();
    }
    | KW_SHOW KW_COLLATION {
        $$ = new ShowCollationSentence();
    }
    | KW_SHOW KW_GROUPS {
        $$ = new ListGroupsSentence();
    }
    | KW_SHOW KW_ZONES {
        $$ = new ListZonesSentence();
    }
    | KW_SHOW KW_STATS {
        $$ = new ShowStatsSentence();
    }
    | KW_SHOW KW_TEXT KW_SEARCH KW_CLIENTS {
        $$ = new ShowTSClientsSentence();
    }
    ;

list_host_type
    : KW_GRAPH      { $$ = meta::cpp2::ListHostType::GRAPH; }
    | KW_META       { $$ = meta::cpp2::ListHostType::META; }
    | KW_STORAGE    { $$ = meta::cpp2::ListHostType::STORAGE; }
    ;

config_module_enum
    : KW_GRAPH      { $$ = meta::cpp2::ConfigModule::GRAPH; }
    | KW_META       { $$ = meta::cpp2::ConfigModule::META; }
    | KW_STORAGE    { $$ = meta::cpp2::ConfigModule::STORAGE; }
    ;

get_config_item
    : config_module_enum COLON name_label {
        $$ = new ConfigRowItem($1, $3);
    }
    | name_label {
        $$ = new ConfigRowItem(meta::cpp2::ConfigModule::ALL, $1);
    }
    ;

set_config_item
    : config_module_enum COLON name_label ASSIGN expression {
        $$ = new ConfigRowItem($1, $3, $5);
    }
    | name_label ASSIGN expression {
        $$ = new ConfigRowItem(meta::cpp2::ConfigModule::ALL, $1, $3);
    }
    | config_module_enum COLON name_label ASSIGN L_BRACE update_list R_BRACE {
        $$ = new ConfigRowItem($1, $3, $6);
    }
    | name_label ASSIGN L_BRACE update_list R_BRACE {
        $$ = new ConfigRowItem(meta::cpp2::ConfigModule::ALL, $1, $4);
    }
    ;

show_config_item
    : %empty {
        $$ = nullptr;
    }
    | config_module_enum {
        $$ = new ConfigRowItem($1);
    }
    ;

zone_name_list
    : name_label {
        $$ = new ZoneNameList();
        $$->addZone($1);
    }
    | zone_name_list COMMA name_label {
        $$ = $1;
        $$->addZone($3);
    }
    ;

create_space_sentence
    : KW_CREATE KW_SPACE opt_if_not_exists name_label {
        auto sentence = new CreateSpaceSentence($4, $3);
        $$ = sentence;
    }
    | KW_CREATE KW_SPACE opt_if_not_exists name_label KW_ON name_label {
        auto sentence = new CreateSpaceSentence($4, $3);
        sentence->setOpts(new SpaceOptList());
        sentence->setGroupName(*$6);
        $$ = sentence;
        delete $6;
    }
    | KW_CREATE KW_SPACE opt_if_not_exists name_label L_PAREN space_opt_list R_PAREN {
        auto sentence = new CreateSpaceSentence($4, $3);
        sentence->setOpts($6);
        $$ = sentence;
    }
    | KW_CREATE KW_SPACE opt_if_not_exists name_label L_PAREN space_opt_list R_PAREN KW_ON name_label {
        auto sentence = new CreateSpaceSentence($4, $3);
        sentence->setOpts($6);
        sentence->setGroupName(*$9);
        $$ = sentence;
        delete $9;
    }
    ;

describe_space_sentence
    : KW_DESCRIBE KW_SPACE name_label {
        $$ = new DescribeSpaceSentence($3);
    }
    | KW_DESC KW_SPACE name_label {
        $$ = new DescribeSpaceSentence($3);
    }
    ;

space_opt_list
    : space_opt_item {
        $$ = new SpaceOptList();
        $$->addOpt($1);
    }
    | space_opt_list COMMA space_opt_item {
        $$ = $1;
        $$->addOpt($3);
    }
    | space_opt_list COMMA {
        $$ = $1;
    }
    ;

space_opt_item
    : KW_PARTITION_NUM ASSIGN legal_integer {
        $$ = new SpaceOptItem(SpaceOptItem::PARTITION_NUM, $3);
    }
    | KW_REPLICA_FACTOR ASSIGN legal_integer {
        $$ = new SpaceOptItem(SpaceOptItem::REPLICA_FACTOR, $3);
    }
    | KW_CHARSET ASSIGN name_label {
        // Currently support utf8, it is an alias for utf8mb4
        $$ = new SpaceOptItem(SpaceOptItem::CHARSET, *$3);
        delete $3;
    }
    | KW_COLLATE ASSIGN name_label {
        // Currently support utf8_bin, it is an alias for utf8mb4_bin
        $$ = new SpaceOptItem(SpaceOptItem::COLLATE, *$3);
        delete $3;
    }
    | KW_VID_TYPE ASSIGN type_spec {
        $$ = new SpaceOptItem(SpaceOptItem::VID_TYPE, *$3);
        delete $3;
    }
    | KW_ATOMIC_EDGE ASSIGN BOOL {
        $$ = new SpaceOptItem(SpaceOptItem::ATOMIC_EDGE, $3);
    }
    // TODO(YT) Create Spaces for different engines
    // KW_ENGINE_TYPE ASSIGN name_label
    ;

drop_space_sentence
    : KW_DROP KW_SPACE opt_if_exists name_label {
        $$ = new DropSpaceSentence($4, $3);
    }
    ;

//  User manager sentences.

create_user_sentence
    : KW_CREATE KW_USER opt_if_not_exists name_label KW_WITH KW_PASSWORD STRING {
        $$ = new CreateUserSentence($4, $7, $3);
    }
    | KW_CREATE KW_USER opt_if_not_exists name_label {
        $$ = new CreateUserSentence($4, new std::string(""), $3);
    }
    ;

alter_user_sentence
    : KW_ALTER KW_USER name_label KW_WITH KW_PASSWORD STRING {
        $$ = new AlterUserSentence($3, $6);
    }
    ;

drop_user_sentence
    : KW_DROP KW_USER opt_if_exists name_label {
        auto sentence = new DropUserSentence($4, $3);
        $$ = sentence;
    }
    ;

change_password_sentence
    : KW_CHANGE KW_PASSWORD name_label KW_FROM STRING KW_TO STRING {
        auto sentence = new ChangePasswordSentence($3, $5, $7);
        $$ = sentence;
    }
    ;

role_type_clause
    : KW_GOD { $$ = new RoleTypeClause(meta::cpp2::RoleType::GOD); }
    | KW_ADMIN { $$ = new RoleTypeClause(meta::cpp2::RoleType::ADMIN); }
    | KW_DBA { $$ = new RoleTypeClause(meta::cpp2::RoleType::DBA); }
    | KW_USER { $$ = new RoleTypeClause(meta::cpp2::RoleType::USER); }
    | KW_GUEST { $$ = new RoleTypeClause(meta::cpp2::RoleType::GUEST); }
    ;

acl_item_clause
    : KW_ROLE role_type_clause KW_ON name_label {
        auto sentence = new AclItemClause(true, $4);
        sentence->setRoleTypeClause($2);
        $$ = sentence;
    }
    | role_type_clause KW_ON name_label {
        auto sentence = new AclItemClause(false, $3);
        sentence->setRoleTypeClause($1);
        $$ = sentence;
    }
    ;

grant_sentence
    : KW_GRANT acl_item_clause KW_TO name_label {
        auto sentence = new GrantSentence($4);
        sentence->setAclItemClause($2);
        $$ = sentence;
    }
    ;

revoke_sentence
    : KW_REVOKE acl_item_clause KW_FROM name_label {
        auto sentence = new RevokeSentence($4);
        sentence->setAclItemClause($2);
        $$ = sentence;
    }
    ;

get_config_sentence
    : KW_GET KW_CONFIGS get_config_item {
        $$ = new GetConfigSentence($3);
    }
    ;

set_config_sentence
    : KW_UPDATE KW_CONFIGS set_config_item {
        $$ = new SetConfigSentence($3);
    }
    ;

host_list
    : host_item {
        $$ = new HostList();
        $$->addHost($1);
    }
    | host_list COMMA host_item {
        $$ = $1;
        $$->addHost($3);
    }
    | host_list COMMA {
        $$ = $1;
    }
    ;

host_item
    : IPV4 COLON port {
        $$ = new nebula::HostAddr();
        $$->host = std::move(*$1);
        delete $1;
        $$->port = $3;
    }
    | STRING COLON port {
        $$ = new nebula::HostAddr();
        $$->host = std::move(*$1);
        delete $1;
        $$->port = $3;
    }

port : INTEGER {
        if ($1 > std::numeric_limits<uint16_t>::max()) {
            throw nebula::GraphParser::syntax_error(@1, "Out of range:");
        }
        $$ = $1;
    }
    ;

integer_list
    : legal_integer {
        $$ = new std::vector<int32_t>();
        $$->emplace_back($1);
    }
    | integer_list COMMA INTEGER {
        $$ = $1;
        $$->emplace_back($3);
    }
    | integer_list COMMA {
        $$ = $1;
    }
    ;

balance_sentence
    : KW_BALANCE KW_LEADER {
        $$ = new BalanceSentence(BalanceSentence::SubType::kLeader);
    }
    | KW_BALANCE KW_DATA {
        $$ = new BalanceSentence(BalanceSentence::SubType::kData);
    }
    | KW_BALANCE KW_DATA legal_integer {
        $$ = new BalanceSentence($3);
    }
    | KW_BALANCE KW_DATA KW_STOP {
        $$ = new BalanceSentence(BalanceSentence::SubType::kDataStop);
    }
    | KW_BALANCE KW_DATA KW_RESET KW_PLAN {
        $$ = new BalanceSentence(BalanceSentence::SubType::kDataReset);
    }
    | KW_BALANCE KW_DATA KW_REMOVE host_list {
        $$ = new BalanceSentence(BalanceSentence::SubType::kData, $4);
    }
    ;

create_snapshot_sentence
    : KW_CREATE KW_SNAPSHOT {
        $$ = new CreateSnapshotSentence();
    }
    ;

drop_snapshot_sentence
    : KW_DROP KW_SNAPSHOT name_label {
        $$ = new DropSnapshotSentence($3);
    }
    ;

add_listener_sentence
    : KW_ADD KW_LISTENER KW_ELASTICSEARCH host_list {
        $$ = new AddListenerSentence(meta::cpp2::ListenerType::ELASTICSEARCH, $4);
    }
    ;

remove_listener_sentence
    : KW_REMOVE KW_LISTENER KW_ELASTICSEARCH {
        $$ = new RemoveListenerSentence(meta::cpp2::ListenerType::ELASTICSEARCH);
    }
    ;

list_listener_sentence
    : KW_SHOW KW_LISTENER {
        $$ = new ShowListenerSentence();
    }
    ;

mutate_sentence
    : insert_vertex_sentence { $$ = $1; }
    | insert_edge_sentence { $$ = $1; }
    | update_vertex_sentence { $$ = $1; }
    | update_edge_sentence { $$ = $1; }
    | download_sentence { $$ = $1; }
    | ingest_sentence { $$ = $1; }
    | admin_job_sentence { $$ = $1; }
    ;

maintain_sentence
    : create_space_sentence { $$ = $1; }
    | describe_space_sentence { $$ = $1; }
    | drop_space_sentence { $$ = $1; }
    | create_tag_sentence { $$ = $1; }
    | create_edge_sentence { $$ = $1; }
    | alter_tag_sentence { $$ = $1; }
    | alter_edge_sentence { $$ = $1; }
    | describe_tag_sentence { $$ = $1; }
    | describe_edge_sentence { $$ = $1; }
    | drop_tag_sentence { $$ = $1; }
    | drop_edge_sentence { $$ = $1; }
    | create_tag_index_sentence { $$ = $1; }
    | create_edge_index_sentence { $$ = $1; }
    | drop_tag_index_sentence { $$ = $1; }
    | drop_edge_index_sentence { $$ = $1; }
    | describe_tag_index_sentence { $$ = $1; }
    | describe_edge_index_sentence { $$ = $1; }
    | rebuild_tag_index_sentence { $$ = $1; }
    | rebuild_edge_index_sentence { $$ = $1; }
    | add_group_sentence { $$ = $1; }
    | drop_group_sentence { $$ = $1; }
    | desc_group_sentence { $$ = $1; }
    | add_zone_into_group_sentence { $$ = $1; }
    | drop_zone_from_group_sentence { $$ = $1; }
    | add_zone_sentence { $$ = $1; }
    | drop_zone_sentence { $$ = $1; }
    | desc_zone_sentence { $$ = $1; }
    | add_host_into_zone_sentence { $$ = $1; }
    | drop_host_from_zone_sentence { $$ = $1; }
    | show_sentence { $$ = $1; }
    | create_user_sentence { $$ = $1; }
    | alter_user_sentence { $$ = $1; }
    | drop_user_sentence { $$ = $1; }
    | change_password_sentence { $$ = $1; }
    | grant_sentence { $$ = $1; }
    | revoke_sentence { $$ = $1; }
    | get_config_sentence { $$ = $1; }
    | set_config_sentence { $$ = $1; }
    | balance_sentence { $$ = $1; }
    | add_listener_sentence { $$ = $1; }
    | remove_listener_sentence { $$ = $1; }
    | list_listener_sentence { $$ = $1; }
    | create_snapshot_sentence { $$ = $1; }
    | drop_snapshot_sentence { $$ = $1; }
    | sign_in_text_search_service_sentence { $$ = $1; }
    | sign_out_text_search_service_sentence { $$ = $1; }
    ;

return_sentence
    : KW_RETURN VARIABLE KW_IF VARIABLE KW_IS KW_NOT KW_NULL {
        $$ = new ReturnSentence($2, $4);
    }

process_control_sentence
    : return_sentence { $$ = $1; }
    ;

sentence
    : maintain_sentence { $$ = $1; }
    | use_sentence { $$ = $1; }
    | set_sentence { $$ = $1; }
    | assignment_sentence { $$ = $1; }
    | mutate_sentence { $$ = $1; }
    | process_control_sentence { $$ = $1; }
    ;

seq_sentences
    : sentence {
        $$ = new SequentialSentences($1);
    }
    | seq_sentences SEMICOLON sentence {
        if ($1 == nullptr) {
            $$ = new SequentialSentences($3);
        } else {
            $1->addSentence($3);
            $$ = $1;
        }
    }
    | seq_sentences SEMICOLON {
        $$ = $1;
    }
    | %empty {
        $$ = nullptr;
    }
    ;

explain_sentence
    : KW_EXPLAIN sentence {
        $$ = new ExplainSentence(new SequentialSentences($2));
    }
    | KW_EXPLAIN KW_FORMAT ASSIGN STRING sentence {
        $$ = new ExplainSentence(new SequentialSentences($5), false, $4);
    }
    | KW_EXPLAIN L_BRACE seq_sentences R_BRACE {
        $$ = new ExplainSentence($3);
    }
    | KW_EXPLAIN KW_FORMAT ASSIGN STRING L_BRACE seq_sentences R_BRACE {
        $$ = new ExplainSentence($6, false, $4);
    }
    | KW_PROFILE sentence {
        $$ = new ExplainSentence(new SequentialSentences($2), true);
    }
    | KW_PROFILE KW_FORMAT ASSIGN STRING sentence {
        $$ = new ExplainSentence(new SequentialSentences($5), true, $4);
    }
    | KW_PROFILE L_BRACE seq_sentences R_BRACE {
        $$ = new ExplainSentence($3, true);
    }
    | KW_PROFILE KW_FORMAT ASSIGN STRING L_BRACE seq_sentences R_BRACE {
        $$ = new ExplainSentence($6, true, $4);
    }
    | explain_sentence SEMICOLON {
        $$ = $1;
    }
    ;

sentences
    : seq_sentences {
        $$ = $1;
        *sentences = $$;
    }
    | explain_sentence {
        $$ = $1;
        *sentences = $$;
    }
    ;

%%

void nebula::GraphParser::error(const nebula::GraphParser::location_type& loc,
                                const std::string &msg) {
    std::ostringstream os;
    if (msg.empty()) {
        os << "syntax error";
    } else {
        os << msg;
    }

    auto *query = scanner.query();
    if (query == nullptr) {
        os << " at " << loc;
        errmsg = os.str();
        return;
    }

    auto begin = loc.begin.column > 0 ? loc.begin.column - 1 : 0;
    if ((loc.end.filename
        && (!loc.begin.filename
            || *loc.begin.filename != *loc.end.filename))
        || loc.begin.line < loc.end.line
        || begin >= query->size()) {
        os << " at " << loc;
    } else if (loc.begin.column < (loc.end.column ? loc.end.column - 1 : 0)) {
        uint32_t len = loc.end.column - loc.begin.column;
        if (len > 80) {
            len = 80;
        }
        os << " near `" << query->substr(begin, len) << "'";
    } else {
        os << " near `" << query->substr(begin, 8) << "'";
    }

    errmsg = os.str();
}

// check the positive integer boundary
// parameter input accept the INTEGER value
// which filled as uint64_t
// so the conversion is expected
void ifOutOfRange(const int64_t input,
                  const nebula::GraphParser::location_type& loc) {
    if ((uint64_t)input >= MAX_ABS_INTEGER) {
        throw nebula::GraphParser::syntax_error(loc, "Out of range:");
    }
}

static int yylex(nebula::GraphParser::semantic_type* yylval,
                 nebula::GraphParser::location_type *yylloc,
                 nebula::GraphScanner& scanner) {
    auto token = scanner.yylex(yylval, yylloc);
    if (scanner.hasUnaryMinus() && token != nebula::GraphParser::token::INTEGER) {
        scanner.setUnaryMinus(false);
    }
    return token;
}
