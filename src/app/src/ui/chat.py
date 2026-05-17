import streamlit as st

from src.services.agent_client import AgentClient, AgentClientError

SESSION_MESSAGES_KEY = "agent_chat_messages"


def render_agent_chat(client: AgentClient) -> None:
    st.subheader("ИИ-агент")

    if SESSION_MESSAGES_KEY not in st.session_state:
        st.session_state[SESSION_MESSAGES_KEY] = [
            {
                "role": "assistant",
                "content": "Готов работать с аналитикой JobLake.",
            }
        ]

    messages: list[dict[str, str]] = st.session_state[SESSION_MESSAGES_KEY]
    for message in messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    prompt = st.chat_input("Вопрос по вакансиям, навыкам, компаниям или зарплатам")
    if not prompt:
        return

    messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    history = messages[:-1][-12:]
    with st.chat_message("assistant"):
        with st.spinner("Агент обращается к данным..."):
            try:
                answer = client.chat(prompt, history=history)
            except AgentClientError as exc:
                answer = f"Не удалось получить ответ агента: {exc}"
            st.markdown(answer)

    messages.append({"role": "assistant", "content": answer})
