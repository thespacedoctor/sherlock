{{ objname }} *(module)*
{{ underline }}===========



.. automodule:: {{ fullname }}
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:
    :member-order: groupwise
    
    {% block classes %}
    {% if classes %}
    .. rubric:: Classes

    .. autosummary::
    {% for item in classes %}
      ~{{ item }}
    {%- endfor %}
    {% endif %}
    {% endblock %}

    {% block functions %}
    {% if functions %}
    .. rubric:: Functions

    .. autosummary::
    {% for item in functions %}
      ~{{ item }}
    {%- endfor %}
    {% endif %}
    {% endblock %}

    {% block attributes %}
    {% if attributes %}
    .. rubric:: Properties

    .. autosummary::
    {% for item in attributes %}
      ~{{ item }}
    {%- endfor %}
    {% endif %}
    {% endblock %}

    {% block members %}
    {% if members %}
    .. rubric:: Sub-modules

    .. autosummary::
    {% for item in members %}
    {% if "__" not in item and "_" not in item|first and "absolute_import" not in item  %}
        {% if "test" not in item %}
            ~{{ item }} (nice)
       {% endif %}
    {% endif %}
    {%- endfor %}
    {% endif %}
    
    {% endblock %}
