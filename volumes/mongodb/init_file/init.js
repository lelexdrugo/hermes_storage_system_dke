db.auth('admin-user', 'admin-password')

db.createUser({
    user: 'gabriele',
    pwd: 'test',
    roles: [
        {
            role: 'readWrite',
            db: 'test-database',
        },
    ],
});


print('User created successfully');