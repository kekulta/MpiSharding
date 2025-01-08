module MpiSharding.User

open System
open System.Data.Common
open MPI
open Npgsql

open MpiSharding.Base

type UserEntity = { Id: string; FirstName: string; LastName: string; Age: string; Sex: string; City: string }

let userInserter execute =
    let insertUsers users =
        try
            execute "BEGIN;" |> ignore
            users |> List.iter (fun user ->
                sprintf "INSERT INTO users VALUES (%s, '%s', '%s', %s, '%s', '%s');"
                        user.Id user.FirstName user.LastName user.Age user.Sex user.City
                        |> execute
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
    let cities = [| "Kazan"; "Moscow"; "New-York"; "Berlin" |];
    let sexes = [| "M"; "F" |];
    let lastNames = [| "abbott"; "acosta"; "adams"; "adkins"; "aguilar" |];

    let rand (arr: string array) =
        arr[random.Next(arr.Length - 1)]

    let rec generate list x =
        match x with
            | 0 -> list
            | _ -> generate ( {
                    Id = (string (random.Next()));
                    FirstName = (rand firstNames);
                    LastName = (rand lastNames); 
                    City = (rand cities); 
                    Sex = (rand sexes); 
                    Age = (string (random.Next(21, 99))); 
                    } :: list ) (x - 1)

    generate [] n

let userReader read =
    let selectString = "SELECT * FROM users;" 
    let makeUserEntity (reader: DbDataReader) 
                                    = { UserEntity.Id = string reader["id"]; 
                                        FirstName = string reader["first_name"]; 
                                        Age = string reader["age"];
                                        Sex = string reader["sex"];
                                        City = string reader["city"];
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
                                last_name VARCHAR(40),
                                age integer,
                                sex VARCHAR(1),
                                city VARCHAR(40)
                                );"
    execute createString |> ignore

