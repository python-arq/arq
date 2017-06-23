from arq import Actor, concurrent


class RegistrationEmail(Actor):
    @concurrent
    async def email_standard_user(self, user_id):
        send_user_email(user_id)

    @concurrent(Actor.HIGH_QUEUE)
    async def email_premium_user(self, user_id):
        send_user_email(user_id)
