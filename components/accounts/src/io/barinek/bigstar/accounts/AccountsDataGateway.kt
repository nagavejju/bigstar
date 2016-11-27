package io.barinek.bigstar.accounts

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.stereotype.Repository
import org.springframework.transaction.support.TransactionTemplate
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.util.*
import javax.inject.Inject

@Repository
open class AccountsDataGateway {
    private val jdbcTemplate: JdbcTemplate
    private val transactionTemplate: TransactionTemplate

    @Inject
    constructor(jdbcTemplate: JdbcTemplate, transactionTemplate: TransactionTemplate) {
        this.jdbcTemplate = jdbcTemplate
        this.transactionTemplate = transactionTemplate
    }

    fun getAccounts(): List<Account> {
        return jdbcTemplate.query("select id, name, total_contract_value, analyzer_identifier, last_analyzed_at from accounts",
                { rs, rowNum ->
                    Account(
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getDouble("total_contract_value"),
                            rs.getString("analyzer_identifier"),
                            rs.getTimestamp("last_analyzed_at").toInstant())
                }
        )
    }

    fun analyzeTenAccount(action: (Account) -> Unit): String {
        val uuid = UUID.randomUUID()

        transactionTemplate.execute { action ->

            val dataSourceTransactionManager = transactionTemplate.transactionManager as DataSourceTransactionManager

            val template = JdbcTemplate(dataSourceTransactionManager.dataSource)

            template.query(
                    { connection ->
                        connection.prepareStatement(
                                """
                                select id, name, total_contract_value, analyzer_identifier, last_analyzed_at
                                  from accounts
                                  where last_analyzed_at < (now() - interval 5 minute)
                                  limit 10
                                  for update
                                """,
                                ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_UPDATABLE
                        )
                    },
                    { rs ->
                        action(Account(
                                rs.getLong("id"),
                                rs.getString("name"),
                                rs.getDouble("total_contract_value"),
                                rs.getString("analyzer_identifier"),
                                rs.getTimestamp("last_analyzed_at").toInstant())

                        )
                        rs.updateString("analyzer_identifier", uuid.toString())
                        rs.updateTimestamp("last_analyzed_at", Timestamp.from(Instant.now()))
                        rs.updateRow()
                    }
            )
        }
        return uuid.toString()
    }

    fun getTwoAccountsForAnalysis(): List<Account> {
        val analyzer_identifier = UUID.randomUUID()

        jdbcTemplate.update(
                { connection ->
                    val statement = connection.prepareStatement(
                            """
                                update accounts
                                  set last_analyzed_at = now(),
                                  analyzer_identifier = ?
                                  where last_analyzed_at < (now() - interval 5 minute)
                                  limit 2
                                """
                    )
                    statement.setString(1, analyzer_identifier.toString())
                    statement
                }
        )

        return jdbcTemplate.query(
                "select id, name, total_contract_value, analyzer_identifier, last_analyzed_at from accounts where analyzer_identifier = ?",
                arrayOf(analyzer_identifier.toString()))
        { rs, rowNum ->
            Account(
                    rs.getLong("id"),
                    rs.getString("name"),
                    rs.getDouble("total_contract_value"),
                    rs.getString("analyzer_identifier"),
                    rs.getTimestamp("last_analyzed_at").toInstant())
        }
    }
}