module MpiSharding.Shard

open System
open System.Data.Common
open MPI
open Npgsql

open MPI

let shardsNum = 2

let shardUserInserter (comm: Intracommunicator) generateUsers insertUsers =
    let shardInsertUsers n =
        let excess = n % shardsNum
        let usersNum = 
            if(comm.Rank = 0) then n / shardsNum + excess
            else n / shardsNum
        printf "Generating %d users in %d proccess.\n" usersNum comm.Rank
        generateUsers usersNum |> insertUsers

    shardInsertUsers

let shardMaxAgeProvider (comm: Intracommunicator) (read: string -> (DbDataReader -> string) -> string list): unit -> int =
    let selectString = "SELECT MAX(age) FROM users;"

    let shardMaxAge () =
        let _, value = Int32.TryParse ((read selectString (fun db -> string db["age"]))[0])
        value
        (* comm.Reduce value Operation<int>.Max 0 *)

    shardMaxAge
