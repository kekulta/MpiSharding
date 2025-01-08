module MpiSharding.Shard

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
