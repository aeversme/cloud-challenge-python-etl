def publish_message(topic, subject, message):
    response = topic.publish(
        Subject=subject,
        Message=message
    )
    return response['MessageId']
