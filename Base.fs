module MpiSharding.Base

open System
open System.Data.Common
open MPI
open Npgsql

let postgresConnection port username =
    sprintf "Host=localhost; Port=%d; Database=mpi_db; User Id=%s; Password=;" port username

let openConn str =
    let conn = new NpgsqlConnection(str)
    conn.Open()
    conn

let executor conn =
    let execute str =
        use cmd = (new NpgsqlCommand(str, conn))
        cmd.ExecuteNonQuery() |> ignore
    execute

let reader conn =
    let read str maker =
        use cmd = (new NpgsqlCommand(str, conn))
        use reader = cmd.ExecuteReader ()

        let rec read list =
            match reader.Read() with
                | true -> read ((maker reader) :: list)
                | _ -> list

        read []
    read


let dropper execute =
    let drop name = 
        sprintf "DROP TABLE %s;" name |> execute
    drop

let truncater execute =
    let truncate name = 
        sprintf "TRUNCATE TABLE %s;" name |> execute
    truncate

let formattedReader read maker =
    let reader str =
        read str maker

    reader
