module MpiSharding.Base

open System
open System.Data.Common
open MPI
open Npgsql

let postgresConnection (port: int) (username: string): string =
    sprintf "Host=localhost; Port=%d; Database=mpi_db; User Id=%s; Password=;" port username

let openConn (str: string): NpgsqlConnection  =
    let conn = new NpgsqlConnection(str)
    conn.Open()
    conn

let executor (conn: NpgsqlConnection): string -> unit =
    let execute str =
        use cmd = (new NpgsqlCommand(str, conn))
        cmd.ExecuteNonQuery() |> ignore
    execute

let reader (conn: NpgsqlConnection): string -> (DbDataReader -> 'a) -> 'a list =
    let read str maker =
        use cmd = (new NpgsqlCommand(str, conn))
        use reader = cmd.ExecuteReader ()

        let rec read list =
            match reader.Read() with
                | true -> read ((maker reader) :: list)
                | _ -> list

        read []
    read


let dropper (execute: string -> unit) =
    let drop name = 
        sprintf "DROP TABLE %s;" name |> execute
    drop

let truncater (execute: string -> unit) =
    let truncate name = 
        sprintf "TRUNCATE TABLE %s;" name |> execute
    truncate

let formattedReader<'a> (read: string -> (DbDataReader -> 'a) -> 'a list) (maker: DbDataReader -> 'a) =
    let reader str =
        read str maker

    reader
