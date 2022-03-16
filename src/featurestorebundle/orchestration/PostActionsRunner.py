from typing import List
from logging import Logger
from featurestorebundle.orchestration.PostActionInterface import PostActionInterface
from daipecore.decorator.ContainerManager import ContainerManager


class PostActionsRunner:
    def __init__(self, logger: Logger, post_actions_service_identifiers: List[str]):
        self.__logger = logger
        self.__post_actions_service_identifiers = [] if post_actions_service_identifiers is None else post_actions_service_identifiers

    def run(self):
        self.__validate_post_action_service_identifiers()

        post_action_services = self.__get_post_action_services()

        self.__validate_post_action_services(post_action_services)

        if not self.__post_actions_service_identifiers:
            self.__logger.info("No post actions")

        for post_action_service, post_action_service_identifier in zip(post_action_services, self.__post_actions_service_identifiers):
            self.__logger.info(f"Running post action {post_action_service_identifier}")

            post_action_service.run()

            self.__logger.info(f"Post action {post_action_service_identifier} done")

    def __get_post_action_services(self) -> List[PostActionInterface]:
        return [ContainerManager.get_container().get(identifier[1:]) for identifier in self.__post_actions_service_identifiers]

    def __validate_post_action_service_identifiers(self):
        for post_action_service_identifier in self.__post_actions_service_identifiers:
            if not post_action_service_identifier.startswith("@"):
                raise Exception("Post Action service identifier must start with '@'")

    def __validate_post_action_services(self, post_action_services: List[PostActionInterface]):
        for post_action_service in post_action_services:
            if not isinstance(post_action_service, PostActionInterface):
                raise Exception(f"Post action service {post_action_service} does not implement PostActionInterface")
