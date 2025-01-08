module MpiSharding.User

open System
open System.Data.Common
open MPI
open Npgsql

open MpiSharding.Base

type UserEntity = { Id: string; FirstName: string; LastName: string }

let userInserter execute =
    let insertUsers users =
        try
            execute "BEGIN;" |> ignore
            users |> List.iter (fun user ->
                (* sprintf "INSERT INTO users VALUES (%s, '%s', '%s');" user.Id user.FirstName user.LastName |> printf "%s" *)
                sprintf "INSERT INTO users VALUES (%s, '%s', '%s');" user.Id user.FirstName user.LastName |> execute
                |> ignore
                ) |> ignore
            execute "COMMIT;" |> ignore
        with
            | _ -> 
            execute "ROLLBACK;" |> ignore
            reraise ()

    insertUsers

let generateUsers n =
    let random = Random()

    let firstNames = [| "aaron"; "abdul"; "abe"; "abel"; "abraham"; "adam"; "adan"; "adolfo"; "adolph"; "adrian"; "abby"; "abigail"; "adele"; "adrian" |];
    let lastNames = [| "abbott"; "acosta"; "adams"; "adkins"; "aguilar" |];

    let rec generate list x =
        match x with
            | 0 -> list
            | _ -> generate ( {
                    Id = (string (random.Next()));
                    FirstName = firstNames[random.Next(firstNames.Length - 1)];
                    LastName = lastNames[random.Next(lastNames.Length - 1)]; 
                    } :: list ) (x - 1)

    generate [] n

let userReader read =
    let selectString = "SELECT * FROM users;" 
    let makeUserEntity (reader: DbDataReader) 
                                    = { UserEntity.Id = string reader["id"]; 
                                        FirstName = string reader["first_name"]; 
                                        LastName = string reader["last_name"] } 
    let reader =
        formattedReader read makeUserEntity
    let uReader () =
        reader selectString

    uReader

let ensureTableExists execute =
    let createString = "CREATE TABLE IF NOT EXISTS users (
                                id integer PRIMARY KEY,
                                first_name VARCHAR(40),
                                last_name VARCHAR(40)
                                );"
    execute createString |> ignore

