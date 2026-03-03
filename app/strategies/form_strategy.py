"""
Form Action Extraction Strategy.

Finds <form> elements with search-related action attributes and extracts
the search URL pattern directly from the form action — no need to
actually submit anything.
"""
import logging
from urllib.parse import urlparse, urljoin
from app.strategies.base import BaseSearchStrategy

logger = logging.getLogger(__name__)


class FormActionStrategy(BaseSearchStrategy):

    async def execute(self):
        """
        Scan all <form> elements for search-related actions.
        Extract the action URL and convert to a search pattern.
        """
        try:
            forms = await self.page.query_selector_all("form")

            for form in forms:
                try:
                    action = await form.get_attribute("action")
                    method = await form.get_attribute("method")
                    role = await form.get_attribute("role")

                    # Check if form is search-related
                    is_search_form = False

                    # Role-based check
                    if role and role.lower() == "search":
                        is_search_form = True

                    # Action-based check
                    if action and any(kw in action.lower() for kw in [
                        "search", "find", "query", "lookup", "seek"
                    ]):
                        is_search_form = True

                    # Class/ID check on the form itself
                    form_class = await form.get_attribute("class") or ""
                    form_id = await form.get_attribute("id") or ""
                    if any(kw in (form_class + form_id).lower() for kw in [
                        "search", "find", "query"
                    ]):
                        is_search_form = True

                    if not is_search_form:
                        continue

                    # Find the input field name inside this form
                    inputs = await form.query_selector_all(
                        "input[type='text'], input[type='search'], input:not([type])"
                    )

                    input_name = None
                    for inp in inputs:
                        inp_type = await inp.get_attribute("type")
                        if inp_type in ["hidden", "submit", "button", "email", "password"]:
                            continue
                        name = await inp.get_attribute("name")
                        if name:
                            input_name = name
                            break

                    if not action:
                        continue

                    # Build the pattern
                    # If action is relative, make it absolute then back to relative
                    if action.startswith("http"):
                        parsed = urlparse(action)
                        action_path = parsed.path
                        if parsed.query:
                            action_path += "?" + parsed.query
                    else:
                        action_path = action

                    # Construct pattern
                    if input_name:
                        # GET form: /search?q={}
                        if not method or method.upper() == "GET":
                            if "?" in action_path:
                                pattern = f"{action_path}&{input_name}={{}}"
                            else:
                                pattern = f"{action_path}?{input_name}={{}}"
                        else:
                            # POST form — still extract the URL
                            pattern = action_path
                    else:
                        # No named input — just use the action URL
                        pattern = action_path

                    logger.info(
                        f"📝 Found search form: action={action}, "
                        f"input_name={input_name}, pattern={pattern}"
                    )

                    return {
                        "method": "form",
                        "pattern": pattern,
                        "confidence": 5,
                        "result_type": "form-action"
                    }

                except Exception as e:
                    logger.debug(f"Error processing form: {e}")
                    continue

        except Exception as e:
            logger.debug(f"FormActionStrategy error: {e}")

        return None
