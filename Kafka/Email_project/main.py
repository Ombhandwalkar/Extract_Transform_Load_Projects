def signup_user():
    while True:
        # We will ask for email from Consumer
        email=input('Enter your email for signup:')
        if email.lower() =='exit':
            print('Exiting signup process ')
            break
        with open('email.txt','a')as f:
            f.write(email + "\n" )

        print(f"Email {email} stored sucessfully ")

if __name__ =="__main__":
    signup_user()