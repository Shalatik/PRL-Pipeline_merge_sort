// Simona Ceskova xcesko00
// 06.04.2024
// PRL 1

#include <bits/stdc++.h>
#include <string.h>
#include <fstream>
#include <iostream>
#include <time.h>
#include <stdio.h>
using namespace std;
#include <assert.h>
#include <queue>
#include <mpi.h>

const int STOP = 0, FIRST = 1, SECOND = 2;

// deque printing at start and at end
void print_queue(deque<int> queue)
{
    for (int i = 0; i < queue.size(); i++)
    {
        cout << queue[i];
        cout << " ";
    }
    cout << "\n";
}

//for printing sorted numbers
void print_output(deque<int> queue)
{
    for (int i = queue.size() - 1; i >= 0; i--)
    {
        cout << queue[i];
        cout << "\n";
    }
}

// reading file and loading data into deque
deque<int> read_numbers()
{

    deque<int> deque_numbers;

    ifstream input("numbers", ios::binary);
    if (!input.is_open())
    {
        fprintf(stderr, "Neplatny soubor\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    unsigned char number;
    int int_number;

    // char size is 1byte
    while (input.get(reinterpret_cast<char &>(number)))
    {
        // back to int 0-255
        int_number = static_cast<int>(number);
        deque_numbers.push_back(int_number);
    }
    input.close();
    //print unsorted numbers
    print_queue(deque_numbers);
    return deque_numbers;
}

// recieve number from MPI_Recv for every processor with rank != 0
void recieve_number(int world_rank, bool &flag_recv_continue, deque<int> &FIRST_deque_numbers, deque<int> &SECOND_deque_numbers)
{
    int number;
    MPI_Status status;
    // after message with STOP tag, flag_recv_continue stops waiting for message
    if (flag_recv_continue)
    {
        MPI_Recv(&number, 1, MPI_INT, world_rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        // FIRST tag for first deque
        // SECOND tag for second deque
        if (status.MPI_TAG == FIRST)
            FIRST_deque_numbers.push_back(number);
        else if (status.MPI_TAG == SECOND)
            SECOND_deque_numbers.push_back(number);
        else if (status.MPI_TAG == STOP) // every number from the file was sent
            flag_recv_continue = false;
    }
}

// flag for chaning deque due to algorithm condition
void check_flag(int rank_stop, int &c_first_sent, int &c_second_sent, bool &flag_pass)
{
    // 2^(rank - 1) numbers were sent
    if (c_first_sent == rank_stop && c_second_sent == rank_stop)
    {
        // switching deque
        flag_pass = !flag_pass;
        // both sent the same amount of numbers
        c_first_sent = 0;
        c_second_sent = 0;
    }
}

// repetitive lines of code for getting number from specific deque
int prepare_to_send(deque<int> &ANY_deque_numbers, int &c_first_sent)
{
    int number_to_send = ANY_deque_numbers[0];
    ANY_deque_numbers.pop_front();
    c_first_sent++;
    return number_to_send;
}

// repetitive lines of code for getting number from specific deque and placing into the last FINISH deque
void finishing_deque(deque<int> &ANY_deque_numbers, deque<int> &FINISH_deque_numbers)
{
    FINISH_deque_numbers.push_back(ANY_deque_numbers[0]);
    ANY_deque_numbers.pop_front();
}

// sending number to next processor
void ready_to_send(bool flag_pass, int number_to_send, int world_rank)
{
    // flag_pass decides what deque use - depens on the condition 2^(rank-1) from each deque
    if (flag_pass)
        MPI_Send(&number_to_send, 1, MPI_INT, world_rank + 1, FIRST, MPI_COMM_WORLD);
    else
        MPI_Send(&number_to_send, 1, MPI_INT, world_rank + 1, SECOND, MPI_COMM_WORLD);
}

// first processor with rank == 0 loads file with numbers and starts pipeline with sending numbers
void first_pipeline_proc(int world_rank, bool one_proc)
{
    // loading numbers
    deque<int> deque_numbers = read_numbers();

    //this condition is when on input is only one number
    if (one_proc)
    {
        cout << deque_numbers[0] << "\n";
        return;
    }

    int number_to_send;
    int counter = 0;
    while (deque_numbers.size() != 0)
    {
        number_to_send = deque_numbers[0];
        deque_numbers.pop_front();
        // switching deques
        if (counter % 2 == 0)
        {
            //            data     count datatype   source         tag  communicator
            MPI_Send(&number_to_send, 1, MPI_INT, world_rank + 1, FIRST, MPI_COMM_WORLD);
        }
        else if (counter % 2 == 1)
            MPI_Send(&number_to_send, 1, MPI_INT, world_rank + 1, SECOND, MPI_COMM_WORLD);
        counter++;
    }
    // every number was sent
    MPI_Send(&number_to_send, 1, MPI_INT, world_rank + 1, STOP, MPI_COMM_WORLD);
}

// every processor with rank > 0 and rank < max_rank
void middle_pipeline_proc(int world_rank, int rank_stop)
{
    // variable for storing number to send
    int number_to_send;
    // flags for stopping sending and reciving numbers
    bool flag_recv_continue = true, flag_send_continue = true;
    // two deques for each processor
    deque<int> FIRST_deque_numbers, SECOND_deque_numbers;
    // counters how many numbers was sent from each deque
    int c_first_sent = 0, c_second_sent = 0;
    // flag for switching deques
    bool flag_pass = true;

    // main loop cycle
    while (flag_recv_continue || flag_send_continue)
    {
        // reciving some new number
        recieve_number(world_rank, flag_recv_continue, FIRST_deque_numbers, SECOND_deque_numbers);

        // sending numbers
        // if first deque is full and second has at least one number
        if (((FIRST_deque_numbers.size() >= rank_stop) && (SECOND_deque_numbers.size() != 0)))
        {
            // first number > second number
            if (((FIRST_deque_numbers[0] >= SECOND_deque_numbers[0]) && (c_first_sent < rank_stop)) || c_second_sent >= rank_stop)
                number_to_send = prepare_to_send(FIRST_deque_numbers, c_first_sent);
            else // first number < second number
                number_to_send = prepare_to_send(SECOND_deque_numbers, c_second_sent);

            // sending one number
            ready_to_send(flag_pass, number_to_send, world_rank);
            // checking flag for deque swap or counters for sending equal amount of numbers from both deques
            check_flag(rank_stop, c_first_sent, c_second_sent, flag_pass);
        }
        // no more new numbers
        else if (flag_recv_continue == 0)
        {
            // both deques are empty, end of process
            if ((SECOND_deque_numbers.size() == 0) && (FIRST_deque_numbers.size() == 0))
                flag_send_continue = false;
            else
            {
                // sending the rest of the numbers depending of how many had been sent
                if (FIRST_deque_numbers.size() == 0)
                    number_to_send = prepare_to_send(SECOND_deque_numbers, c_second_sent);
                else if (SECOND_deque_numbers.size() == 0) 
                    number_to_send = prepare_to_send(FIRST_deque_numbers, c_first_sent);
                else if (((FIRST_deque_numbers[0] >= SECOND_deque_numbers[0]) && (c_first_sent < rank_stop)) || c_second_sent >= rank_stop) // first number > second number
                    number_to_send = prepare_to_send(FIRST_deque_numbers, c_first_sent); 
                else // first number < second number
                    number_to_send = prepare_to_send(SECOND_deque_numbers, c_second_sent);

                // sending one number
                ready_to_send(flag_pass, number_to_send, world_rank);
                // checking flag for deque swap or counters for sending equal amount of numbers from both deques
                check_flag(rank_stop, c_first_sent, c_second_sent, flag_pass);
            }
        }
    }
    // STOP message for next processor
    MPI_Send(&number_to_send, 1, MPI_INT, world_rank + 1, STOP, MPI_COMM_WORLD);
    // end of processor with rank world_rank
}

void last_pipeline_proc(int world_rank, int rank_stop)
{
    deque<int> FINISH_deque_numbers, FIRST_deque_numbers, SECOND_deque_numbers;

    // flags for stopping sending and reciving numbers
    bool flag_push_continue = true, flag_recv_continue = true;

    // main loop cycle
    while (flag_push_continue)
    {
        // reciving some new number
        recieve_number(world_rank, flag_recv_continue, FIRST_deque_numbers, SECOND_deque_numbers);

        // storing numbers
        // if first deque is full and second has at least one number
        if (((FIRST_deque_numbers.size() >= rank_stop) && (SECOND_deque_numbers.size() != 0)))
        {
            // first number > second number
            if (FIRST_deque_numbers[0] >= SECOND_deque_numbers[0])
                finishing_deque(FIRST_deque_numbers, FINISH_deque_numbers);
            else // first number < second number
                finishing_deque(SECOND_deque_numbers, FINISH_deque_numbers);
        }
        // no more new numbers
        else if (flag_recv_continue == 0)
        {
            // both deques are empty, end of process
            if ((SECOND_deque_numbers.size() == 0) && (FIRST_deque_numbers.size() == 0))
                flag_push_continue = false;
            else
            {
                // storing the rest of the numbers depending of how many had been sent
                if (FIRST_deque_numbers.size() == 0)
                    finishing_deque(SECOND_deque_numbers, FINISH_deque_numbers);
                else if (SECOND_deque_numbers.size() == 0)
                    finishing_deque(FIRST_deque_numbers, FINISH_deque_numbers);
                else if ((FIRST_deque_numbers[0] >= SECOND_deque_numbers[0])) // first number > second number
                    finishing_deque(FIRST_deque_numbers, FINISH_deque_numbers);
                else // first number < second number
                    finishing_deque(SECOND_deque_numbers, FINISH_deque_numbers);
            }
        }
    }
    // print finished deque
    print_output(FINISH_deque_numbers);
}

int main(int argc, char **argv)
{
    int world_rank, world_size;
    int one_proc = false; //while on input is only one number

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (world_size == 1)
        one_proc = true;

    // condition for processor how many number it should send
    int rank_stop = pow(2, world_rank - 1);

    // process decision
    if (world_rank == 0)
    {
        first_pipeline_proc(world_rank, one_proc);
        if (one_proc)
        {
            //while on input is only one number
            MPI_Finalize();
            return 0;
        }
    }
    else if ((world_rank != 0) && (world_rank != world_size - 1))
        middle_pipeline_proc(world_rank, rank_stop);
    else if (world_rank == world_size - 1)
        last_pipeline_proc(world_rank, rank_stop);

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}