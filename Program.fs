open System
open System.Data.Common
open MPI
open Npgsql

open MpiSharding.Base
open MpiSharding.User
open MpiSharding.Shard
let ports = [|5433; 5434|]
let username = "kekulta"

let args = Environment.GetCommandLineArgs()

type Reader = string -> (DbDataReader -> 'a) -> 'a list

let worker (execute: string -> unit) (read: Reader) (comm: Intracommunicator) =
    ensureTableExists execute |> ignore
    let readUsers = userReader read<UserEntity>
    let truncate = truncater execute
    let insertUsers = userInserter execute
    let shardMaxAge = shardMaxAgeProvider comm read<string>
    let shardInsertUsers = shardUserInserter comm generateUsers insertUsers

    (* generateUsers 20 |> insertUsers *)
    (* readUsers () |> printf "%O" *)
    (* truncate "users" *)
    (* readUsers () |> printf "%O" *)

    
    let (|Prefix|_|) (p:string) (s:string) =
        if s.StartsWith(p) then
            Some(s.Substring(p.Length))
        else
            None

    let numbercheck (candidate : string) =
        let isInt, _ = Int32.TryParse candidate
        let isDouble, _ = Double.TryParse candidate
        isInt || isDouble

    let mutable command = null
    let mutable loop = true

    while loop do
        if comm.Rank = 0 then command <- string (Console.ReadLine ())

        comm.Broadcast(&command, 0)

        match command with
            | Prefix "Generate " rest when numbercheck rest -> 
                let _, number = Int32.TryParse rest
                if comm.Rank = 0 then printfn "Generating %d users...\n" number
                shardInsertUsers number
            | Prefix "Generate" rest -> 
                if comm.Rank = 0 then printfn "Can't generate '%s' users.\n" rest
            | "Max Age" ->
                let maxAge = shardMaxAge ()
                (* if comm.Rank = 0 then *) 
                printfn "Max age is %d\n" maxAge
            | "Clear" ->
                if comm.Rank = 0 then printfn "Clearing db...\n"
                truncate "users"
            | "Exit" | "Quit" | "q" -> 
                if comm.Rank = 0 then printfn "Bye!\n"
                loop <- false
            | _ ->
                if comm.Rank = 0 then printfn "Unknown command!\n"

        comm.Barrier()
    ()

MPI.Environment.Run( ref args, (fun comm -> 
    let rank = comm.Rank
    let port = ports[rank]

    let conn = openConn(postgresConnection port username)
        
    let execute = executor conn
    let read = reader conn

    worker execute read comm

    ()
    )) 

 
