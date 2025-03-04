package dev.lukewang.learnmybatis

import com.zaxxer.hikari.HikariDataSource
import org.springframework.boot.test.context.SpringBootTest
import java.sql.DriverManager
import kotlin.test.Test

@SpringBootTest
class H2DbTests {

    @Test
    fun jdbcTest() {
        DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1")
            .use { conn ->
                val isValid = conn.isValid(0)
                println("isValid: $isValid")

                val ddlExecuted = conn.createStatement()
                    .executeLargeUpdate("create table islands(id int, name varchar(255), country varchar(255))")
                if (ddlExecuted == 0L) println("DDL executed")
                else return

                val insertStatement = conn.prepareStatement("insert into islands(id, name, country) values(?, ?, ?)")
                insertStatement.setInt(1, 1)
                insertStatement.setString(2, "Hawaii")
                insertStatement.setString(3, "USA")
                val affected0 = insertStatement.executeUpdate()
                if (affected0 > 0) println("Inserted $affected0 rows") else return
                insertStatement.setInt(1, 2)
                insertStatement.setString(2, "Bali")
                insertStatement.setString(3, "Indonesia")
                val affected1 = insertStatement.executeUpdate()
                if (affected1 > 0) println("Inserted $affected1 rows") else return

                val selectStatement = conn.prepareStatement("select * from islands")
                val resultSet = selectStatement.executeQuery()

                while (resultSet.next()) {
                    val id = resultSet.getInt("id")
                    val name = resultSet.getString("name")
                    val country = resultSet.getString("country")
                    println("id: $id, name: $name, country: $country")
                }
            }
    }

    @Test
    fun hikariConnPoolTest() {
        val dataSource = HikariDataSource()
        dataSource.jdbcUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1"

        dataSource.connection.use { conn ->
            val isValid = conn.isValid(0)
            println("isValid: $isValid")

            val ddlExecuted = conn.createStatement()
                .executeLargeUpdate("create table islands(id int, name varchar(255), country varchar(255))")
            if (ddlExecuted == 0L) println("DDL executed")
            else return

            val insertStatement = conn.prepareStatement("insert into islands(id, name, country) values(?, ?, ?)")
            insertStatement.setInt(1, 1)
            insertStatement.setString(2, "Hawaii")
            insertStatement.setString(3, "USA")
            val affected0 = insertStatement.executeUpdate()
            if (affected0 > 0) println("Inserted $affected0 rows") else return
            insertStatement.setInt(1, 2)
            insertStatement.setString(2, "Bali")
            insertStatement.setString(3, "Indonesia")
            val affected1 = insertStatement.executeUpdate()
            if (affected1 > 0) println("Inserted $affected1 rows") else return

            val selectStatement = conn.prepareStatement("select * from islands")
            val resultSet = selectStatement.executeQuery()

            while (resultSet.next()) {
                val id = resultSet.getInt("id")
                val name = resultSet.getString("name")
                val country = resultSet.getString("country")
                println("id: $id, name: $name, country: $country")
            }
        }
    }
}