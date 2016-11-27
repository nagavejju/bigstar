package io.barinek.bigstar.accounts

import java.time.Instant

data class Account(val id: Long?, val name: String, val totalContractValue: Double = 0.00, val analyzerIdentifier: String, val analyzedAt: Instant)