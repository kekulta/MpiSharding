open System
open System.Data.Common
open MPI
open Npgsql

open MpiSharding.Base
open MpiSharding.User

let ports = [|5433; 5434|]
let username = "kekulta"

let args = Environment.GetCommandLineArgs()

let worker execute read =
    ensureTableExists execute |> ignore
    let readUsers = userReader read
    let truncate = truncater execute
    let insertUsers = userInserter execute

    generateUsers 20 |> insertUsers
    readUsers () |> printf "%O"
    truncate "users"
    readUsers () |> printf "%O"

    ()

MPI.Environment.Run( ref args, (fun comm -> 
    let rank = comm.Rank
    let port = ports[rank]

    let conn = openConn(postgresConnection port username)
        
    let execute = executor conn
    let read = reader conn

    worker execute read

    ()
    )) 

 
