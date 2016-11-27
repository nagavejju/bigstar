package barinek.bigstar.accounts

import io.barinek.bigstar.accounts.Account
import io.barinek.bigstar.accounts.AccountsDataGateway
import io.barinek.bigstar.jdbc.DataSourceConfig
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import java.time.Instant

open class AccountsDataGatewayTest {
    @Before
    fun setUp() {
        DataSourceConfig().getJdbcTemplate().execute("delete from accounts")
    }

    @Test
    fun testGetAccounts() {
        val jdbcTemplate = DataSourceConfig().getJdbcTemplate()

        jdbcTemplate.update("""
            insert into accounts (name, total_contract_value, analyzer_identifier, last_analyzed_at)
             values
             ('John\'s Grocery, Inc.', 6000000, uuid(), now()),
             ('Hamburg Inn No. 2', 0, uuid(), now()),
             ('Record Collector', 1400000, uuid(), now())
        """)

        val accounts = AccountsDataGateway(jdbcTemplate, DataSourceConfig().getTransactionTemplate()).getAccounts()
        assertEquals(3, accounts.size)
    }

    @Test
    fun testGetAccountsForUpdate() {
        val jdbcTemplate = DataSourceConfig().getJdbcTemplate()

        jdbcTemplate.update("""
            insert into accounts (name, total_contract_value, analyzer_identifier, last_analyzed_at)
             values
             ('John\'s Grocery, Inc.', 6000000, uuid(), (now() - interval 6 minute)),
             ('Hamburg Inn No. 2', 0, uuid(), (now() - interval 6 minute)),
             ('Record Collector', 1400000, uuid(), (now() - interval 6 minute))
        """)

        for (i in 1..500) {
            jdbcTemplate.update("""
            insert into accounts (name, total_contract_value, analyzer_identifier, last_analyzed_at)
             values (uuid(), round(10000.0 * rand(), 2), uuid(), (now() - interval 6 minute))
            """)
        }

        val uuid = AccountsDataGateway(jdbcTemplate, DataSourceConfig().getTransactionTemplate()).analyzeTenAccount { account ->
            val fiveMinutesAgo = Instant.now().minusSeconds(60 * 5)
            assert(account.analyzedAt < fiveMinutesAgo)
        }

        val accounts = jdbcTemplate.query(
                "select id, name, total_contract_value, analyzer_identifier, last_analyzed_at from accounts where analyzer_identifier = ?",
                arrayOf(uuid))
        { rs, n ->
            Account(
                    rs.getLong("id"),
                    rs.getString("name"),
                    rs.getDouble("total_contract_value"),
                    rs.getString("analyzer_identifier"),
                    rs.getTimestamp("last_analyzed_at").toInstant())
        }
        assertEquals(10, accounts.size)
    }

    @Test
    fun testGetUpdatedAccounts() {
        val jdbcTemplate = DataSourceConfig().getJdbcTemplate()

        jdbcTemplate.update("""
            insert into accounts (name, total_contract_value, analyzer_identifier, last_analyzed_at)
             values
             ('John\'s Grocery, Inc.', 6000000, uuid(), (now() - interval 6 minute)),
             ('Hamburg Inn No. 2', 0, uuid(), (now() - interval 6 minute)),
             ('Record Collector', 1400000, uuid(), (now() - interval 6 minute))
        """)

        val accounts = AccountsDataGateway(jdbcTemplate, DataSourceConfig().getTransactionTemplate()).getTwoAccountsForAnalysis()
        assertEquals(2, accounts.size)
    }
}