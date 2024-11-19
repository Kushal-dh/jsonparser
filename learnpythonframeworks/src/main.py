import argparse
import sys

def main(name, surname):
    """
    This is the main function
    """
    print(name, surname)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="add name and surname")
   
    parser.add_argument(
        "--name",
        '-n',
        type=str,
        required= True,
        help="your name"
    )

    parser.add_argument(
        "--surname", 
        "-s",
        # nargs="*",  
        type=str, 
        required=True,
        help="Your surname"
    )

    args = parser.parse_args()
    name = args.name
    surname = args.surname


    # name = sys.argv[1]  # use this for simplicity and without named arguments
    # surname = sys.argv[2] # use this for simplicity and without named arguments
    
    main(name, surname)
